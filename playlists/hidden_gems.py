from playlists.base import BasePlaylist
from utils import db


class HiddenGems(BasePlaylist):

    @property
    def name(self):
        return "lesser listened to tracks #auto"

    @property
    def description(self):
        return "songs from my playlists that are not very popular according to spotify - refreshes approx. every 3 hrs"

    @property
    def public(self):
        return True

    @property
    def add_to_profile(self):
        return True

    @property
    def library_folder(self):
        return "auto playlists"

    def get_tracks(self, sp):
        tracks = []

        try:
            self.logger.info(
                "Fetching random hidden gems (popularity 0-5) from database..."
            )

            songs = db.get_filtered_songs(min_popularity=0, max_popularity=5, limit=50)

            if not songs:
                self.logger.warning("No songs found with popularity 0-5 in database")
                return tracks

            self.logger.info(f"Found {len(songs)} songs in database")

            for song in songs:
                if song.get("uri"):
                    tracks.append(song["uri"])
                    self.logger.info(
                        f"Added hidden gem: {song.get('name', 'Unknown')} (popularity: {song.get('popularity', 'Unknown')})"
                    )
                else:
                    self.logger.warning(
                        f"Song {song.get('name', 'Unknown')} has no URI"
                    )

            self.logger.info(
                f"Successfully prepared {len(tracks)} hidden gem tracks for playlist"
            )

        except Exception as e:
            self.logger.error(f"Error fetching hidden gems from database: {e}")

        return tracks
