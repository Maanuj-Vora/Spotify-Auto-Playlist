import dotenv
import spotipy
import time
import requests
from functools import wraps
from collections import deque
from threading import Lock
from utils.logger import Logger

logger = Logger().get_logger()

_REQUESTS_PER_MINUTE = 90
_BATCH_SIZE = 50
_MIN_REQUEST_INTERVAL = 60.0 / _REQUESTS_PER_MINUTE
_request_times = deque()
_artist_batch = []
_rate_limit_lock = Lock()


def _enforce_rate_limit():
    with _rate_limit_lock:
        current_time = time.time()

        while _request_times and current_time - _request_times[0] > 60:
            _request_times.popleft()

        if len(_request_times) >= _REQUESTS_PER_MINUTE:
            sleep_time = 60 - (current_time - _request_times[0]) + 0.1
            if sleep_time > 0:
                logger.info(
                    f"Rate limit approached, sleeping for {sleep_time:.2f} seconds"
                )
                time.sleep(sleep_time)
                current_time = time.time()

                while _request_times and current_time - _request_times[0] > 60:
                    _request_times.popleft()

        if _request_times:
            time_since_last = current_time - _request_times[-1]
            if time_since_last < _MIN_REQUEST_INTERVAL:
                sleep_time = _MIN_REQUEST_INTERVAL - time_since_last
                time.sleep(sleep_time)
                current_time = time.time()

        _request_times.append(current_time)


def rate_limited_call(func, *args, **kwargs):
    try:
        _enforce_rate_limit()
        return func(*args, **kwargs)
    except spotipy.exceptions.SpotifyException as e:
        if e.http_status >= 500:
            logger.warning(
                f"Server error {e.http_status}: {e}. Waiting before continuing..."
            )
            time.sleep(5)
        check_for_rate_limit_error(e)
        raise
    except Exception as e:
        check_for_rate_limit_error(e)
        raise


def add_artist_to_batch(artist_id):
    if artist_id not in _artist_batch:
        _artist_batch.append(artist_id)


def process_artist_batch(sp):
    if not _artist_batch:
        return {}

    logger.info(f"Processing {len(_artist_batch)} artists in batches of {_BATCH_SIZE}")
    results = {}

    for i in range(0, len(_artist_batch), _BATCH_SIZE):
        batch = _artist_batch[i : i + _BATCH_SIZE]
        logger.info(
            f"Fetching batch of {len(batch)} artists (batch {i//_BATCH_SIZE + 1})"
        )

        try:
            artists_data = rate_limited_call(sp.artists, batch)

            for artist in artists_data["artists"]:
                if artist:
                    results[artist["id"]] = artist
        except spotipy.exceptions.SpotifyException as e:
            check_for_rate_limit_error(e)
            raise
        except Exception as e:
            check_for_rate_limit_error(e)
            raise

    _artist_batch.clear()
    logger.info(f"Successfully processed {len(results)} artists")
    return results


def create_validation_response(
    valid=False, accessible=False, error=None, data_key=None, data_value=None
):
    response = {"valid": valid, "accessible": accessible, "error": error}

    if data_key:
        response[data_key] = data_value

    return response


def handle_spotify_exception(e, entity_type="entity"):
    if e.http_status == 404:
        return {
            "valid": False,
            "accessible": False,
            "error": f"{entity_type.title()} not found or not accessible",
        }
    elif e.http_status == 403:
        return {
            "valid": True,
            "accessible": False,
            "error": f"{entity_type.title()} exists but is private/not accessible",
        }
    elif e.http_status == 401:
        return {
            "valid": False,
            "accessible": False,
            "error": "Authentication error - check Spotify credentials",
        }
    else:
        return {
            "valid": False,
            "accessible": False,
            "error": f"Spotify API error: {str(e)}",
        }


def create_accessibility_response(accessible=False, error=None, **extra_data):
    response = {"accessible": accessible, "error": error}
    response.update(extra_data)
    return response


def check_for_rate_limit_error(e):
    error_message = str(e).lower()
    if "rate" in error_message and "limit" in error_message:
        logger.error(
            "Spotify rate limit exceeded. This indicates the application is making too many requests."
        )
        logger.error(
            "The batching system should prevent this, but it has been triggered anyway."
        )
        logger.error("Exiting application to prevent further rate limit violations.")
        logger.error(f"Error details: {str(e)}")
        exit(1)

    if hasattr(e, "http_status") and e.http_status == 429:
        logger.error(
            "Spotify API returned HTTP 429 (Too Many Requests) - rate limit exceeded."
        )
        logger.error("Exiting application to prevent further rate limit violations.")
        logger.error(f"Error details: {str(e)}")
        exit(1)


def retry_on_timeout(max_retries=3, delay=30, use_rate_limiting=True):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    if (
                        use_rate_limiting
                        and len(args) > 0
                        and hasattr(args[0], "client_credentials_manager")
                    ):
                        return rate_limited_call(func, *args, **kwargs)
                    else:
                        return func(*args, **kwargs)
                except requests.exceptions.ReadTimeout as e:
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Spotify API timeout (attempt {attempt + 1}/{max_retries}): {str(e)}"
                        )
                        logger.info(f"Waiting {delay} seconds before retry...")
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"Spotify API timeout after {max_retries} attempts, giving up: {str(e)}"
                        )
                        raise
                except spotipy.exceptions.SpotifyException as e:
                    check_for_rate_limit_error(e)
                    logger.error(f"Spotify API error in {func.__name__}: {str(e)}")
                    raise
                except Exception as e:
                    check_for_rate_limit_error(e)
                    logger.error(f"Non-timeout error in {func.__name__}: {str(e)}")
                    raise
            return None

        return wrapper

    return decorator


@retry_on_timeout(max_retries=3, delay=30)
def get_user_playlists(sp, env_path=".env", username=None):
    playlists = sp.user_playlists(username)
    playlist_objs = []

    while playlists:
        for i, playlist in enumerate(playlists["items"]):
            print(
                f"{i + 1 + playlists['offset']:4d} {playlist['uri']} {playlist['name']}"
            )
            playlist_objs.append(playlist)
        if playlists["next"]:
            playlists = sp.next(playlists)
        else:
            playlists = None

    return playlist_objs


@retry_on_timeout(max_retries=3, delay=30)
def get_id_playlist(sp, playlist_id):
    playlist = sp.playlist(playlist_id)
    return playlist


@retry_on_timeout(max_retries=3, delay=30)
def get_playlist_songs(sp, playlist_id):
    tracks = sp.playlist_tracks(playlist_id)
    track_objs = []

    while tracks:
        for item in tracks["items"]:
            if item["track"] and item["track"].get("id") and item["track"].get("name"):
                track_objs.append(item["track"])
            else:
                track_info = item["track"] if item["track"] else "None"
                logger.warning(
                    f"Skipping track with missing essential data: {track_info}"
                )
        if tracks["next"]:
            tracks = sp.next(tracks)
        else:
            tracks = None

    return track_objs


@retry_on_timeout(max_retries=3, delay=30)
def get_artist_info(sp, artist_id):
    artist = sp.artist(artist_id)
    return artist


def queue_artist_for_batch(artist_id):
    add_artist_to_batch(artist_id)


def get_artists_batch(sp, artist_ids=None):
    if artist_ids:
        logger.info(f"Processing specific list of {len(artist_ids)} artists")
        results = {}

        for i in range(0, len(artist_ids), _BATCH_SIZE):
            batch = artist_ids[i : i + _BATCH_SIZE]
            logger.info(f"Fetching batch of {len(batch)} artists")

            try:
                artists_data = rate_limited_call(sp.artists, batch)

                for artist in artists_data["artists"]:
                    if artist:
                        results[artist["id"]] = artist
            except spotipy.exceptions.SpotifyException as e:
                check_for_rate_limit_error(e)
                raise
            except Exception as e:
                check_for_rate_limit_error(e)
                raise

        return results
    else:
        return process_artist_batch(sp)


@retry_on_timeout(max_retries=3, delay=30)
def validate_playlist_id(sp, playlist_id):
    try:
        if not playlist_id or not isinstance(playlist_id, str):
            return create_validation_response(
                error="Invalid playlist ID format: ID must be a non-empty string",
                data_key="playlist_info",
                data_value=None,
            )

        if playlist_id.startswith("spotify:playlist:"):
            playlist_id = playlist_id.split(":")[-1]
        elif playlist_id.startswith("https://open.spotify.com/playlist/"):
            playlist_id = playlist_id.split("/")[-1].split("?")[0]

        playlist = sp.playlist(
            playlist_id, fields="id,name,public,owner.id,collaborative"
        )

        return create_validation_response(
            valid=True, accessible=True, data_key="playlist_info", data_value=playlist
        )

    except spotipy.exceptions.SpotifyException as e:
        error_response = handle_spotify_exception(e, "playlist")
        error_response["playlist_info"] = None
        return error_response
    except Exception as e:
        return create_validation_response(
            error=f"Unexpected error: {str(e)}",
            data_key="playlist_info",
            data_value=None,
        )


@retry_on_timeout(max_retries=3, delay=30)
def validate_user_id(sp, user_id):
    try:
        if not user_id or not isinstance(user_id, str):
            return create_validation_response(
                error="Invalid user ID format: ID must be a non-empty string",
                data_key="user_info",
                data_value=None,
            )

        user_id = user_id.strip()

        if not user_id:
            return create_validation_response(
                error="User ID cannot be empty or whitespace only",
                data_key="user_info",
                data_value=None,
            )

        user = sp.user(user_id)

        return create_validation_response(
            valid=True, accessible=True, data_key="user_info", data_value=user
        )

    except spotipy.exceptions.SpotifyException as e:
        error_response = handle_spotify_exception(e, "user")
        error_response["user_info"] = None
        return error_response
    except Exception as e:
        return create_validation_response(
            error=f"Unexpected error: {str(e)}", data_key="user_info", data_value=None
        )


@retry_on_timeout(max_retries=3, delay=30)
def validate_user_playlists_accessible(sp, user_id):
    try:
        playlists = sp.user_playlists(user_id, limit=1)
        return create_accessibility_response(
            accessible=True, playlist_count=playlists.get("total", 0)
        )
    except spotipy.exceptions.SpotifyException as e:
        if e.http_status == 404:
            return create_accessibility_response(
                error="User not found", playlist_count=None
            )
        elif e.http_status == 403:
            return create_accessibility_response(
                error="User playlists are private or not accessible",
                playlist_count=None,
            )
        else:
            return create_accessibility_response(
                error=f"Error accessing user playlists: {str(e)}", playlist_count=None
            )
    except Exception as e:
        return create_accessibility_response(
            error=f"Unexpected error: {str(e)}", playlist_count=None
        )


def process_tracks_with_batched_artists(sp, tracks):
    artist_ids = set()
    for track in tracks:
        for artist in track.get("artists", []):
            artist_ids.add(artist["id"])

    if artist_ids:
        logger.info(f"Batch fetching info for {len(artist_ids)} unique artists")
        artist_data = get_artists_batch(sp, list(artist_ids))
    else:
        artist_data = {}

    return artist_data


def reset_batch_state():
    global _artist_batch, _request_times
    _artist_batch.clear()
    _request_times.clear()
    logger.info("Batch manager state reset")


def create_playlist_with_tracks(sp, title, description="", tracks=None, public=False):
    try:
        user = sp.current_user()
        user_id = user["id"]
        logger.info(f"Creating playlist '{title}' for user: {user_id}")

        playlist = rate_limited_call(
            sp.user_playlist_create,
            user=user_id,
            name=title,
            description=description,
            public=public,
        )

        playlist_id = playlist["id"]
        logger.info(f"Created playlist '{title}' with ID: {playlist_id}")

        if tracks:
            valid_tracks = validate_tracks(sp, tracks)
            if valid_tracks:
                rate_limited_call(
                    sp.user_playlist_add_tracks,
                    user=user_id,
                    playlist_id=playlist_id,
                    tracks=valid_tracks,
                )
                logger.info(
                    f"Successfully added {len(valid_tracks)} tracks to playlist"
                )
            else:
                logger.warning("No valid tracks found to add to playlist")

        logger.info(
            f"Playlist created successfully! URL: {playlist['external_urls']['spotify']}"
        )
        return playlist

    except Exception as e:
        logger.error(f"Error creating playlist '{title}': {e}")
        return None


def update_playlist_details(sp, playlist_id, title=None, description=None, public=None):
    try:
        update_data = {}

        if title is not None:
            update_data["name"] = title
        if description is not None:
            update_data["description"] = description
        if public is not None:
            update_data["public"] = public

        if not update_data:
            logger.warning("No updates provided for playlist")
            return False

        user = rate_limited_call(sp.current_user)
        user_id = user["id"]

        rate_limited_call(
            sp.user_playlist_change_details,
            user=user_id,
            playlist_id=playlist_id,
            **update_data,
        )

        logger.info(f"Successfully updated playlist {playlist_id}")
        return True

    except Exception as e:
        logger.error(f"Error updating playlist {playlist_id}: {e}")
        return False


def add_tracks_to_playlist(sp, playlist_id, tracks):
    try:
        valid_tracks = validate_tracks(sp, tracks)
        if not valid_tracks:
            logger.warning("No valid tracks to add")
            return False

        rate_limited_call(
            sp.user_playlist_add_tracks, playlist_id=playlist_id, tracks=valid_tracks
        )

        logger.info(
            f"Successfully added {len(valid_tracks)} tracks to playlist {playlist_id}"
        )
        return True

    except Exception as e:
        logger.error(f"Error adding tracks to playlist {playlist_id}: {e}")
        return False


def validate_tracks(sp, tracks):
    valid_tracks = []
    
    if not tracks:
        return valid_tracks
    
    logger.info(f"Validating {len(tracks)} tracks in batches of {_BATCH_SIZE}")
    
    for i in range(0, len(tracks), _BATCH_SIZE):
        batch = tracks[i:i + _BATCH_SIZE]
        batch_ids = []
        batch_uris = []
        
        for track_uri in batch:
            try:
                track_id = track_uri.split(":")[-1]
                batch_ids.append(track_id)
                batch_uris.append(track_uri)
            except Exception as e:
                logger.warning(f"Could not parse track URI {track_uri}: {e}")
        
        if not batch_ids:
            continue
            
        try:
            logger.info(f"Validating batch of {len(batch_ids)} tracks")
            
            tracks_info = rate_limited_call(sp.tracks, batch_ids)
            
            for j, track_info in enumerate(tracks_info.get('tracks', [])):
                if track_info and track_info.get('id'):
                    valid_tracks.append(batch_uris[j])
                    artist_name = track_info.get('artists', [{}])[0].get('name', 'Unknown Artist')
                    logger.info(f"Validated track: {track_info.get('name', 'Unknown')} by {artist_name}")
                else:
                    logger.warning(f"Invalid track in batch: {batch_uris[j] if j < len(batch_uris) else 'Unknown'}")
                    
        except Exception as e:
            logger.error(f"Error validating batch: {e}")
            logger.info("Falling back to individual track validation for this batch")
            for track_uri in batch:
                try:
                    track_id = track_uri.split(":")[-1]
                    track_info = rate_limited_call(sp.track, track_id)
                    
                    if track_info:
                        valid_tracks.append(track_uri)
                        artist_name = track_info.get('artists', [{}])[0].get('name', 'Unknown Artist')
                        logger.info(f"Validated track: {track_info.get('name', 'Unknown')} by {artist_name}")
                        
                except Exception as individual_e:
                    logger.warning(f"Could not validate track {track_uri}: {individual_e}")
    
    logger.info(f"Successfully validated {len(valid_tracks)} out of {len(tracks)} tracks")
    return valid_tracks


def get_playlist_info(sp, playlist_id):
    try:
        playlist = rate_limited_call(sp.playlist, playlist_id)
        logger.info(f"Retrieved info for playlist: {playlist['name']}")
        return playlist

    except Exception as e:
        logger.error(f"Error getting playlist info for {playlist_id}: {e}")
        return None


def playlist_exists_on_spotify(sp, playlist_id):
    try:
        playlist = rate_limited_call(
            sp.playlist, playlist_id, fields="id,name,owner.id"
        )

        current_user = rate_limited_call(sp.current_user)
        current_user_id = current_user.get("id")

        if playlist.get("owner", {}).get("id") != current_user_id:
            logger.info(
                f"Playlist {playlist_id} exists but is not owned by current user"
            )
            return False

        logger.debug(f"Checking if playlist {playlist_id} is in user's library")
        return _check_in_user_playlists(sp, playlist_id)

    except spotipy.exceptions.SpotifyException as e:
        if e.http_status == 404:
            logger.info(f"Playlist {playlist_id} not found (deleted or never existed)")
            return False
        elif e.http_status == 403:
            logger.warning(
                f"Playlist {playlist_id} exists but is not accessible (private)"
            )
            return _check_in_user_playlists(sp, playlist_id)
        elif e.http_status == 401:
            logger.error(f"Authentication error when checking playlist {playlist_id}")
            return False
        else:
            logger.warning(
                f"Spotify API error checking playlist {playlist_id}: HTTP {e.http_status} - {e}"
            )
            return False
    except (ConnectionError, TimeoutError) as e:
        logger.warning(f"Network error checking playlist {playlist_id}: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error checking playlist {playlist_id}: {e}")
        return False


def _check_in_user_playlists(sp, playlist_id):
    try:
        logger.debug(f"Checking user's playlists for {playlist_id}")
        playlists = rate_limited_call(sp.current_user_playlists, limit=50)

        total_checked = 0
        while playlists:
            for playlist in playlists.get("items", []):
                total_checked += 1
                if playlist.get("id") == playlist_id:
                    logger.info(
                        f"Found playlist {playlist_id} in user's library: '{playlist.get('name', 'Unknown')}'"
                    )
                    return True

            if playlists.get("next"):
                playlists = rate_limited_call(sp.next, playlists)
            else:
                break

        logger.info(
            f"Playlist {playlist_id} not found in user's library (checked {total_checked} playlists)"
        )
        return False

    except Exception as e:
        logger.error(f"Error checking user playlists for {playlist_id}: {e}")
        return False


def clear_playlist_tracks(sp, playlist_id):
    try:
        playlist = rate_limited_call(
            sp.playlist, playlist_id, fields="tracks.items(track.uri)"
        )

        if not playlist or not playlist.get("tracks", {}).get("items"):
            logger.info(f"Playlist {playlist_id} is already empty")
            return True

        track_uris = []
        for item in playlist["tracks"]["items"]:
            if item.get("track") and item["track"].get("uri"):
                track_uris.append(item["track"]["uri"])

        if track_uris:
            logger.info(
                f"Removing {len(track_uris)} tracks from playlist {playlist_id}"
            )

            batch_size = 100
            for i in range(0, len(track_uris), batch_size):
                batch = track_uris[i : i + batch_size]
                rate_limited_call(
                    sp.playlist_remove_all_occurrences_of_items, playlist_id, batch
                )
                logger.info(f"Removed batch of {len(batch)} tracks")

            logger.info(f"Successfully cleared all tracks from playlist {playlist_id}")

        return True

    except Exception as e:
        logger.error(f"Error clearing tracks from playlist {playlist_id}: {e}")
        return False


def replace_playlist_tracks(sp, playlist_id, tracks):
    try:
        if not clear_playlist_tracks(sp, playlist_id):
            logger.error(f"Failed to clear existing tracks from playlist {playlist_id}")
            return False

        if tracks:
            valid_tracks = validate_tracks(sp, tracks)
            if valid_tracks:
                rate_limited_call(
                    sp.playlist_add_items, playlist_id=playlist_id, items=valid_tracks
                )
                logger.info(
                    f"Successfully replaced tracks in playlist {playlist_id} with {len(valid_tracks)} new tracks"
                )
                return True
            else:
                logger.warning("No valid tracks to add after clearing playlist")
                return True
        else:
            logger.info(
                f"Successfully cleared playlist {playlist_id} (no new tracks to add)"
            )
            return True

    except Exception as e:
        logger.error(f"Error replacing tracks in playlist {playlist_id}: {e}")
        return False
