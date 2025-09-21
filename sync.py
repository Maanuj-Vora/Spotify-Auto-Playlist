import dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from utils.logger import Logger
from utils import spotify
from utils import db
from utils import config

logger = Logger().get_logger()

dotenv.load_dotenv()
logger.info("Loaded .env file successfully")

if not config.validate_config():
    logger.error("Invalid configuration. Please check config.yml file.")
    exit(1)

auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

logger.info("Initializing database session...")
db.init_session()

logger.info("Ensuring database tables exist...")
db.create_tables()

def get_playlists_to_track():
    usernames = config.get_usernames()
    playlists_to_track = config.get_playlists_to_track()

    playlists = []
    
    if usernames:
        for username in usernames:
            logger.info(f"Validating user: {username}")

            user_validation = spotify.validate_user_id(sp, username)
            if not user_validation['valid']:
                logger.error(f"Invalid user ID '{username}': {user_validation['error']}")
                continue
            
            if not user_validation['accessible']:
                logger.warning(f"User '{username}' exists but is not accessible: {user_validation['error']}")
                continue
            
            playlist_access = spotify.validate_user_playlists_accessible(sp, username)
            if not playlist_access['accessible']:
                logger.warning(f"Cannot access playlists for user '{username}': {playlist_access['error']}")
                continue
            
            logger.info(f"User '{username}' validated successfully. Found {playlist_access['playlist_count']} total playlists.")
            logger.info(f"Fetching playlists for user: {username}")
            
            try:
                user_playlists = spotify.get_user_playlists(sp, username=username)
                if user_playlists:
                    playlists.extend(user_playlists)
                    logger.info(f"Successfully fetched {len(user_playlists)} playlists for user '{username}'")
            except Exception as e:
                logger.error(f"Error fetching playlists for user {username}: {e}")
    
    if playlists_to_track:
        for playlist_id in playlists_to_track:
            logger.info(f"Validating playlist ID: {playlist_id}")
            
            playlist_validation = spotify.validate_playlist_id(sp, playlist_id)
            if not playlist_validation['valid']:
                logger.error(f"Invalid playlist ID '{playlist_id}': {playlist_validation['error']}")
                continue
            
            if not playlist_validation['accessible']:
                logger.warning(f"Playlist '{playlist_id}' exists but is not accessible: {playlist_validation['error']}")
                continue
            
            playlist_info = playlist_validation['playlist_info']
            logger.info(f"Playlist '{playlist_id}' validated successfully: '{playlist_info['name']}' by {playlist_info['owner']['id']}")
            
            try:
                playlist = spotify.get_id_playlist(sp, playlist_id)
                if playlist:
                    playlists.append(playlist)
                    logger.info(f"Successfully fetched playlist '{playlist['name']}' (ID: {playlist_id})")
            except Exception as e:
                logger.error(f"Error fetching playlist by ID {playlist_id}: {e}")

    if len(playlists) == 0:
        logger.info("No valid playlists to track. Exiting.")
        exit(0)
    return playlists

def update_playlists(playlists):
    logger.info("Checking for playlist modifications...")
    modified_playlists = db.get_modified_playlists(playlists)

    if modified_playlists:
        logger.info(f"Found {len(modified_playlists)} modified playlist(s):")
        for playlist in modified_playlists:
            old_snapshot = db.get_playlist_snapshot_id(playlist.get("id"))
            new_snapshot = playlist.get("snapshot_id")
            playlist_name = playlist.get("name")
            playlist_id = playlist.get("id")

            if "#auto" in playlist_name:
                logger.info(f"Skipping auto playlist: '{playlist_name}' (ID: {playlist_id})")
                db.log_action(
                    action_type="SKIP",
                    entity_type="PLAYLIST",
                    entity_id=playlist_id,
                    entity_name=playlist_name,
                    reason="Auto playlist detected - contains '#auto' in name",
                    details="Auto playlists are excluded from sync to prevent sync loops",
                    success=True
                )
                continue
            
            if old_snapshot is None:
                logger.info(f"\t- NEW: '{playlist_name}' (ID: {playlist_id})")
                db.insert_playlist_change(playlist_id, playlist_name, "NEW", None, new_snapshot)
                db.log_action(
                    action_type="ADD_TO_QUEUE",
                    entity_type="PLAYLIST",
                    entity_id=playlist_id,
                    entity_name=playlist_name,
                    reason="New playlist detected - first time tracking",
                    details=f"Snapshot ID: {new_snapshot}",
                    success=True
                )
            else:
                logger.info(f"\t- MODIFIED: '{playlist_name}' (ID: {playlist_id})")
                logger.info(f"\t  Old snapshot: {old_snapshot}")
                logger.info(f"\t  New snapshot: {new_snapshot}")
                db.insert_playlist_change(playlist_id, playlist_name, "MODIFIED", old_snapshot, new_snapshot)
                db.log_action(
                    action_type="ADD_TO_QUEUE",
                    entity_type="PLAYLIST",
                    entity_id=playlist_id,
                    entity_name=playlist_name,
                    reason="Playlist modification detected - snapshot ID changed",
                    details=f"Old snapshot: {old_snapshot}, New snapshot: {new_snapshot}",
                    success=True
                )
    else:
        logger.info("No playlist modifications detected.")
        db.log_action(
            action_type="CHECK_COMPLETE",
            entity_type="SYSTEM",
            entity_id=None,
            entity_name="Playlist Modification Check",
            reason="Completed checking all tracked playlists for modifications",
            details=f"Checked {len(playlists)} playlists, no modifications found",
            success=True
        )

    logger.info("Updating playlist database...")
    for playlist in playlists:
        db.insert_playlist(playlist)

def update_songs():
    modified_playlists = db.get_queue()

    for playlist in modified_playlists:
        playlist_id = playlist['playlist_id']
        playlist_name = playlist['playlist_name']
        logger.info(f"Processing queued playlist: {playlist_name}")
        
        logger.info(f"Validating accessibility for playlist: {playlist_name}")
        playlist_validation = spotify.validate_playlist_id(sp, playlist_id)
        
        if not playlist_validation['valid']:
            logger.error(f"Playlist '{playlist_name}' (ID: {playlist_id}) is no longer valid: {playlist_validation['error']}")
            db.log_action(
                action_type="REMOVE_FROM_QUEUE",
                entity_type="PLAYLIST", 
                entity_id=playlist_id,
                entity_name=playlist_name,
                reason="Playlist validation failed - no longer exists or invalid ID",
                details=f"Validation error: {playlist_validation['error']}",
                success=True
            )
            db.delete_queue(playlist_id)
            logger.info(f"Removed invalid playlist '{playlist_name}' from processing queue")
            continue
        
        if not playlist_validation['accessible']:
            logger.warning(f"Playlist '{playlist_name}' (ID: {playlist_id}) is no longer accessible: {playlist_validation['error']}")
            db.log_action(
                action_type="REMOVE_FROM_QUEUE",
                entity_type="PLAYLIST",
                entity_id=playlist_id, 
                entity_name=playlist_name,
                reason="Playlist became inaccessible - private or permissions changed",
                details=f"Accessibility error: {playlist_validation['error']}",
                success=True
            )
            db.delete_queue(playlist_id)
            logger.info(f"Removed inaccessible playlist '{playlist_name}' from processing queue")
            continue
        
        logger.info(f"Playlist '{playlist_name}' is accessible. Starting sync...")
        db.log_action(
            action_type="SYNC_START",
            entity_type="PLAYLIST",
            entity_id=playlist_id,
            entity_name=playlist_name,
            reason="Playlist passed validation checks and is ready for sync",
            success=True
        )
        
        try:
            tracks = spotify.get_playlist_songs(sp, playlist_id)
            current_song_ids = []
            new_tracks = []
            
            for track in tracks:
                if not track or not track.get('id') or not track.get('name'):
                    logger.warning(f"Skipping track with missing data: {track}")
                    continue
                    
                track_name = track['name']
                track_id = track['id']
                logger.info(f"Processing track: {track_name} (ID: {track_id})")
                current_song_ids.append(track_id)

                if db.get_song_by_id(track_id) is None:
                    logger.info(f"\t- New song detected: {track_name} (ID: {track_id})")
                    new_tracks.append(track)
                else:
                    logger.info(f"\t- Existing song: {track_name} (ID: {track_id})")
            
            if new_tracks:
                logger.info(f"Batch processing artist info for {len(new_tracks)} new tracks")
                artist_data = spotify.process_tracks_with_batched_artists(sp, new_tracks)
                
                for track in new_tracks:
                    track_id = track['id']
                    db.insert_song(track)

                    artists = track['artists']
                    for artist in artists:
                        logger.info(f"\tProcessing artist: {artist['name']} (ID: {artist['id']})")
                        artist_id = artist['id']
                        
                        if artist_id in artist_data:
                            artist_info = artist_data[artist_id]
                            db.insert_artist(artist_info)
                        else:
                            logger.warning(f"Artist {artist_id} not found in batch, fetching individually")
                            artist_info = spotify.get_artist_info(sp, artist_id)
                            db.insert_artist(artist_info)
                        
                        db.insert_song_artist(track_id, artist_id)
            
            sync_result = db.sync_playlist_songs(playlist_id, current_song_ids)
            
            logger.info(f"Playlist sync complete for '{playlist_name}':")
            logger.info(f"\t- Songs added: {sync_result['added']}")
            logger.info(f"\t- Songs removed: {sync_result['removed']}")

            if sync_result['songs_added']:
                logger.info(f"\t- Added song IDs: {sync_result['songs_added']}")
            if sync_result['songs_removed']:
                logger.info(f"\t- Removed song IDs: {sync_result['songs_removed']}")

            db.log_action(
                action_type="SYNC_COMPLETE",
                entity_type="PLAYLIST",
                entity_id=playlist_id,
                entity_name=playlist_name,
                reason="Playlist sync completed successfully",
                details=f"Added: {sync_result['added']} songs, Removed: {sync_result['removed']} songs",
                success=True
            )

            logger.info(f"Removing '{playlist_name}' from processing queue...")
            db.delete_queue(playlist_id)
            logger.info(f"Successfully removed '{playlist_name}' from queue.")
            
        except Exception as e:
            logger.error(f"Error syncing playlist '{playlist_name}': {e}")
            db.log_action(
                action_type="SYNC_FAILED",
                entity_type="PLAYLIST",
                entity_id=playlist_id,
                entity_name=playlist_name,
                reason="Playlist sync failed due to unexpected error",
                details=f"Error: {str(e)}",
                success=False,
                error_message=str(e)
            )
            continue

def cleanup_orphaned_data(currently_tracked_playlists):
    logger.info("Starting comprehensive orphan cleanup...")
    
    try:
        currently_tracked_ids = [playlist['id'] for playlist in currently_tracked_playlists]
        logger.info(f"Currently tracking {len(currently_tracked_ids)} playlist(s)")
        
        cleanup_stats = {
            'orphaned_playlists': {'count': 0, 'deleted': 0, 'songs_removed': 0, 'changes_removed': 0},
            'orphaned_playlist_songs': 0,
            'orphaned_songs': {'count': 0, 'deleted': 0},
            'orphaned_song_artists': 0,
            'orphaned_artists': {'count': 0, 'deleted': 0}
        }
        
        logger.info("Cleaning up orphaned playlists...")
        try:
            orphaned_playlists = db.get_orphaned_playlists(currently_tracked_ids)
            cleanup_stats['orphaned_playlists']['count'] = len(orphaned_playlists)
            
            if orphaned_playlists:
                logger.info(f"\tFound {len(orphaned_playlists)} orphaned playlist(s) to remove")
                
                for playlist in orphaned_playlists:
                    playlist_id = playlist['id']
                    playlist_name = playlist['name']
                    
                    try:
                        logger.info(f"\t- Removing orphaned playlist: '{playlist_name}' (ID: {playlist_id})")
                        
                        playlist_songs = db.get_songs_in_playlist(playlist_id)
                        queue = db.get_queue_by_id(playlist_id)
                        
                        db.delete_playlist_and_relationships(playlist_id)
                        
                        cleanup_stats['orphaned_playlists']['deleted'] += 1
                        cleanup_stats['orphaned_playlists']['songs_removed'] += len(playlist_songs)
                        cleanup_stats['orphaned_playlists']['changes_removed'] += len(queue)
                    except Exception as e:
                        logger.error(f"\t- Failed to delete playlist '{playlist_name}': {e}")
            else:
                logger.info("\tNo orphaned playlists found")
        except Exception as e:
            logger.error(f"\tError during orphaned playlist cleanup: {e}")
        
        logger.info("Cleaning up orphaned playlist-song relationships...")
        try:
            cleanup_stats['orphaned_playlist_songs'] = db.delete_orphaned_playlist_songs()
            if cleanup_stats['orphaned_playlist_songs'] > 0:
                logger.info(f"\tRemoved {cleanup_stats['orphaned_playlist_songs']} orphaned playlist-song relationship(s)")
            else:
                logger.info("\tNo orphaned playlist-song relationships found")
        except Exception as e:
            logger.error(f"\tError cleaning up orphaned playlist-song relationships: {e}")
        
        logger.info("Cleaning up orphaned songs...")
        try:
            orphaned_songs = db.get_orphaned_songs()
            cleanup_stats['orphaned_songs']['count'] = len(orphaned_songs)
            
            if orphaned_songs:
                logger.info(f"Found {len(orphaned_songs)} orphaned song(s) to delete")
                
                for song in orphaned_songs:
                    song_id = song['id']
                    song_name = song['name']
                    
                    try:
                        logger.info(f"\t- Deleting orphaned song: '{song_name}' (ID: {song_id})")
                        db.delete_song(song_id)
                        cleanup_stats['orphaned_songs']['deleted'] += 1
                    except Exception as e:
                        logger.error(f"\t- Failed to delete song '{song_name}': {e}")
            else:
                logger.info("\tNo orphaned songs found")
        except Exception as e:
            logger.error(f"\tError during orphaned songs cleanup: {e}")

        logger.info("Cleaning up orphaned song-artist relationships...")
        try:
            cleanup_stats['orphaned_song_artists'] = db.delete_orphaned_song_artists()
            if cleanup_stats['orphaned_song_artists'] > 0:
                logger.info(f"\tRemoved {cleanup_stats['orphaned_song_artists']} orphaned song-artist relationship(s)")
            else:
                logger.info("\tNo orphaned song-artist relationships found")
        except Exception as e:
            logger.error(f"\tError cleaning up orphaned song-artist relationships: {e}")
        
        logger.info("Cleaning up orphaned artists...")
        try:
            orphaned_artists = db.get_orphaned_artists()
            cleanup_stats['orphaned_artists']['count'] = len(orphaned_artists)
            
            if orphaned_artists:
                logger.info(f"\tFound {len(orphaned_artists)} orphaned artist(s) to delete")
                
                for artist in orphaned_artists:
                    artist_id = artist['id']
                    artist_name = artist['name']
                    
                    try:
                        logger.info(f"\t- Deleting orphaned artist: '{artist_name}' (ID: {artist_id})")
                        db.delete_artist(artist_id)
                        cleanup_stats['orphaned_artists']['deleted'] += 1
                    except Exception as e:
                        logger.error(f"\t- Failed to delete artist '{artist_name}': {e}")
            else:
                logger.info("\tNo orphaned artists found")
        except Exception as e:
            logger.error(f"\tError during orphaned artists cleanup: {e}")
        
        logger.info("Orphan cleanup results:")
        
        playlist_stats = cleanup_stats['orphaned_playlists']
        if playlist_stats['count'] > 0:
            logger.info(f"\tOrphaned playlists: {playlist_stats['deleted']}/{playlist_stats['count']} deleted")
            logger.info(f"\t\tPlaylist-song relationships removed: {playlist_stats['songs_removed']}")
            logger.info(f"\t\tChange records removed: {playlist_stats['changes_removed']}")
        else:
            logger.info("\tNo orphaned playlists found")
        
        if cleanup_stats['orphaned_playlist_songs'] > 0:
            logger.info(f"\tOrphaned playlist-song relationships: {cleanup_stats['orphaned_playlist_songs']} cleaned")
        else:
            logger.info("\tNo orphaned playlist-song relationships found")
        
        song_stats = cleanup_stats['orphaned_songs']
        if song_stats['count'] > 0:
            logger.info(f"\tOrphaned songs: {song_stats['deleted']}/{song_stats['count']} deleted")
        else:
            logger.info("\tNo orphaned songs found")

        if cleanup_stats['orphaned_song_artists'] > 0:
            logger.info(f"\tOrphaned song-artist relationships: {cleanup_stats['orphaned_song_artists']} cleaned")
        else:
            logger.info("\tNo orphaned song-artist relationships found")
        artist_stats = cleanup_stats['orphaned_artists']
        if artist_stats['count'] > 0:
            logger.info(f"\tOrphaned artists: {artist_stats['deleted']}/{artist_stats['count']} deleted")
        else:
            logger.info("\tNo orphaned artists found")

        total_cleaned = (playlist_stats['deleted'] + cleanup_stats['orphaned_playlist_songs'] +
                        cleanup_stats['orphaned_song_artists'] + song_stats['deleted'] + artist_stats['deleted'])
        
        if total_cleaned > 0:
            logger.info(f"Cleanup complete: {total_cleaned} total items removed")
        else:
            logger.info("Cleanup complete: Database is already clean")

    except Exception as e:
        logger.error(f"Error during comprehensive orphan cleanup: {e}")
        raise e


logger.info("=== SPOTIFY PLAYLIST SYNC STARTED ===")

try:
    db.log_action(
        action_type="SYNC_SESSION_START",
        entity_type="SYSTEM",
        entity_id=None,
        entity_name="Sync Session",
        reason="Starting new sync session",
        success=True
    )

    logger.info("Loading playlists to track...")
    playlists = get_playlists_to_track()
    logger.info(f"Total playlists to track: {len(playlists)}")

    logger.info("Starting playlist update process...")
    update_playlists(playlists=playlists)
    logger.info("Playlist update process complete.")

    logger.info("Starting song update process...")
    update_songs()
    logger.info("Song update process complete.")

    logger.info("Starting comprehensive orphan cleanup...")
    cleanup_orphaned_data(playlists)
    logger.info("Comprehensive orphan cleanup complete.")

    logger.info("=== SPOTIFY PLAYLIST SYNC COMPLETED ===")
    db.log_action(
        action_type="SYNC_SESSION_COMPLETE",
        entity_type="SYSTEM",
        entity_id=None,
        entity_name="Sync Session",
        reason="Sync session completed successfully",
        details=f"Processed {len(playlists)} playlists",
        success=True
    )

except spotipy.exceptions.SpotifyException as e:
    error_message = str(e).lower()
    if "rate" in error_message and "limit" in error_message:
        logger.error("Spotify rate limit exceeded during sync process.")
        logger.error("The application will exit to prevent further rate limit violations.")
        logger.error(f"Error details: {str(e)}")
        exit(1)
    elif hasattr(e, 'http_status') and e.http_status == 429:
        logger.error("Spotify API returned HTTP 429 (Too Many Requests) during sync process.")
        logger.error("The application will exit to prevent further rate limit violations.")
        logger.error(f"Error details: {str(e)}")
        exit(1)
    else:
        logger.error(f"Spotify API error during sync: {str(e)}")
        raise
except Exception as e:
    error_message = str(e).lower()
    if "rate" in error_message and "limit" in error_message:
        logger.error("Rate limit error detected during sync process.")
        logger.error("The application will exit to prevent further rate limit violations.")
        logger.error(f"Error details: {str(e)}")
        exit(1)
    else:
        logger.error(f"Unexpected error during sync: {str(e)}")
        raise
    
finally:
    logger.info("Closing database session...")
    db.close_session()