import os
import json
import importlib
import inspect
import dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from utils.logger import Logger
from utils import config
from utils import spotify
from utils import db
from playlists.base import BasePlaylist

logger = Logger().get_logger()

dotenv.load_dotenv()
logger.info("Loaded .env file successfully")

if not config.validate_config():
    logger.error("Invalid configuration. Please check config.yml file.")
    exit(1)

logger.info("Initializing database session...")
db.init_session()

logger.info("Ensuring database tables exist...")
db.create_tables()

cache_data = None
if os.getenv("SPOTIFY_REFRESH_TOKEN"):
    logger.info("Using refresh token from environment variable")
    cache_data = {
        "access_token": "",
        "token_type": "Bearer",
        "expires_in": 3600,
        "refresh_token": os.getenv("SPOTIFY_REFRESH_TOKEN"),
        "scope": "playlist-modify-public playlist-modify-private",
        "expires_at": 0,
    }

    with open(".cache", "w") as f:
        json.dump(cache_data, f)
else:
    logger.info("No refresh token found, will use normal OAuth flow")

auth_manager = SpotifyOAuth(
    scope="playlist-modify-public playlist-modify-private",
    redirect_uri="http://localhost:8080",
)
sp = spotipy.Spotify(auth_manager=auth_manager)

logger.info("Spotify authentication successful")


def discover_playlist_classes():
    playlist_classes = []
    playlists_dir = "playlists"

    for filename in os.listdir(playlists_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            module_name = filename[:-3]

            try:
                module = importlib.import_module(f"playlists.{module_name}")

                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if (
                        issubclass(obj, BasePlaylist)
                        and obj != BasePlaylist
                        and obj.__module__ == module.__name__
                    ):
                        playlist_classes.append(obj)
                        logger.info(
                            f"Discovered playlist class: {name} in {module_name}.py"
                        )

            except Exception as e:
                logger.warning(f"Could not load playlist module {module_name}: {e}")

    return playlist_classes


def cleanup_unmanaged_playlists(playlist_classes):
    logger.info("Cleaning up unmanaged playlists from database...")

    current_filenames = set()
    for playlist_class in playlist_classes:
        filename = playlist_class.__module__.split(".")[-1]
        current_filenames.add(filename)

    managed_playlists = db.get_all_managed_playlists()

    removed_count = 0
    for playlist_record in managed_playlists:
        filename = playlist_record["filename"]

        if filename not in current_filenames:
            logger.info(
                f"Removing unmanaged playlist from database: {filename} (ID: {playlist_record['playlist_id']})"
            )
            db.delete_managed_playlist(filename)
            removed_count += 1

    if removed_count > 0:
        logger.info(f"Cleaned up {removed_count} unmanaged playlist(s) from database")
    else:
        logger.info("No unmanaged playlists found in database")


def create_all_playlists():
    logger.info("Starting playlist creation/update process...")

    playlist_classes = discover_playlist_classes()

    if not playlist_classes:
        logger.warning("No playlist classes found in playlists directory")
        return []

    cleanup_unmanaged_playlists(playlist_classes)

    managed_playlists = []

    for playlist_class in playlist_classes:
        try:
            playlist_instance = playlist_class()

            playlist = playlist_instance.create_or_update_playlist(sp, spotify)

            if playlist:
                managed_playlists.append(playlist)
                logger.info(f"Managed: {playlist['name']}")
            else:
                logger.error(f"Failed to manage: {playlist_instance.name}")

        except Exception as e:
            logger.error(f"Error managing playlist from {playlist_class.__name__}: {e}")

    return managed_playlists


logger.info("Starting playlist management orchestrator...")

managed_playlists = create_all_playlists()

if managed_playlists:
    logger.info(f"Successfully managed {len(managed_playlists)} playlists")
    for playlist in managed_playlists:
        print(f"  - {playlist['name']}: {playlist['external_urls']['spotify']}")
else:
    logger.error("No playlists were managed")
