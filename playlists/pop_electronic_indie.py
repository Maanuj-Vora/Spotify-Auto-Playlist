from playlists.base import BasePlaylist


class Pop_Electronic_Indie(BasePlaylist):

    @property
    def name(self):
        return "Pop, Electronic, and Indie Mix #auto"

    @property
    def description(self):
        return (
            "dynamically created playlist based on search terms: pop, electronic, indie"
        )

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
            search_terms = ["pop", "electronic", "indie"]
            for term in search_terms:
                self.logger.info(f"Searching for tracks with term: {term}")
                results = sp.search(
                    q=f"genre:{term}", type="track", limit=50, market="US"
                )
                for track in results["tracks"]["items"]:
                    track_uri = track["uri"]
                    if track_uri not in tracks:
                        tracks.append(track_uri)
                        self.logger.info(
                            f"Found track: {track['name']} by {track['artists'][0]['name']}"
                        )
                    if len(tracks) >= 50:
                        break
                if len(tracks) >= 50:
                    break
        except Exception as e:
            self.logger.warning(f"Error searching for dynamic tracks: {e}")
        return tracks
