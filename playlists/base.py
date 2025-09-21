from abc import ABC, abstractmethod
from utils.logger import Logger
from utils import db

logger = Logger().get_logger()


class BasePlaylist(ABC):

    def __init__(self):
        self.logger = logger
        self.filename = self.__class__.__module__.split(".")[-1]

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def description(self):
        pass

    @property
    def public(self):
        return False

    @property
    def add_to_profile(self):
        return False

    @property
    def library_folder(self):
        return None

    @abstractmethod
    def get_tracks(self, sp):
        pass

    def create_or_update_playlist(self, sp, spotify_utils):
        try:
            managed_playlist = db.get_managed_playlist(self.filename)

            if managed_playlist:
                playlist_id = managed_playlist["playlist_id"]
                self.logger.info(
                    f"Found existing managed playlist '{self.name}' with ID: {playlist_id}"
                )

                if spotify_utils.playlist_exists_on_spotify(sp, playlist_id):
                    needs_update = (
                        managed_playlist["title"] != self.name
                        or managed_playlist["description"] != self.description
                        or managed_playlist["public"] != int(self.public)
                        or managed_playlist.get("add_to_profile", 0)
                        != int(self.add_to_profile)
                        or managed_playlist.get("library_folder") != self.library_folder
                    )

                    if needs_update:
                        self.logger.info(f"Updating playlist details for '{self.name}'")
                        success = spotify_utils.update_playlist_details(
                            sp,
                            playlist_id,
                            title=self.name,
                            description=self.description,
                            public=self.public,
                        )

                        if success:
                            db.save_managed_playlist(
                                self.filename,
                                playlist_id,
                                self.name,
                                self.description,
                                self.public,
                                self.add_to_profile,
                                self.library_folder,
                            )
                            self.logger.info(
                                f"Successfully updated playlist '{self.name}'"
                            )
                        else:
                            self.logger.error(
                                f"Failed to update playlist '{self.name}'"
                            )
                            return None

                    tracks = self.get_tracks(sp)
                    self.logger.info(f"Replacing tracks in playlist '{self.name}'")

                    success = spotify_utils.replace_playlist_tracks(
                        sp, playlist_id, tracks
                    )
                    if success:
                        self.logger.info(
                            f"Successfully updated tracks for playlist '{self.name}'"
                        )
                    else:
                        self.logger.error(
                            f"Failed to update tracks for playlist '{self.name}'"
                        )
                        return None

                    return spotify_utils.get_playlist_info(sp, playlist_id)
                else:
                    self.logger.warning(
                        f"Playlist {playlist_id} no longer exists on Spotify, creating new one"
                    )
                    db.delete_managed_playlist(self.filename)

            self.logger.info(f"Creating new playlist: {self.name}")
            tracks = self.get_tracks(sp)

            playlist = spotify_utils.create_playlist_with_tracks(
                sp=sp,
                title=self.name,
                description=self.description,
                tracks=tracks,
                public=self.public,
            )

            if playlist:
                playlist_id = playlist["id"]
                db.save_managed_playlist(
                    self.filename,
                    playlist_id,
                    self.name,
                    self.description,
                    self.public,
                    self.add_to_profile,
                    self.library_folder,
                )
                self.logger.info(
                    f"Successfully created and saved managed playlist '{self.name}' with ID: {playlist_id}"
                )

                # Log the new properties for visibility
                if self.add_to_profile:
                    self.logger.info(
                        f"Playlist '{self.name}' marked for profile display"
                    )
                if self.library_folder:
                    self.logger.info(
                        f"Playlist '{self.name}' organized in folder: '{self.library_folder}'"
                    )

                return playlist
            else:
                self.logger.error(f"Failed to create playlist '{self.name}'")
                return None

        except Exception as e:
            self.logger.error(f"Error creating/updating playlist '{self.name}': {e}")
            return None
