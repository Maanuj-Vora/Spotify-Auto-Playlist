import dotenv
import spotipy
import time
import requests
from functools import wraps
from utils.logger import Logger

logger = Logger().get_logger()

def create_validation_response(valid=False, accessible=False, error=None, data_key=None, data_value=None):
    response = {
        'valid': valid,
        'accessible': accessible,
        'error': error
    }
    
    if data_key:
        response[data_key] = data_value
    
    return response

def handle_spotify_exception(e, entity_type="entity"):
    if e.http_status == 404:
        return {
            'valid': False,
            'accessible': False,
            'error': f'{entity_type.title()} not found or not accessible'
        }
    elif e.http_status == 403:
        return {
            'valid': True,
            'accessible': False,
            'error': f'{entity_type.title()} exists but is private/not accessible'
        }
    elif e.http_status == 401:
        return {
            'valid': False,
            'accessible': False,
            'error': 'Authentication error - check Spotify credentials'
        }
    else:
        return {
            'valid': False,
            'accessible': False,
            'error': f'Spotify API error: {str(e)}'
        }

def create_accessibility_response(accessible=False, error=None, **extra_data):
    response = {
        'accessible': accessible,
        'error': error
    }
    response.update(extra_data)
    return response

def retry_on_timeout(max_retries=3, delay=30):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.ReadTimeout as e:
                    if attempt < max_retries - 1:
                        logger.warning(f"Spotify API timeout (attempt {attempt + 1}/{max_retries}): {str(e)}")
                        logger.info(f"Waiting {delay} seconds before retry...")
                        time.sleep(delay)
                    else:
                        logger.error(f"Spotify API timeout after {max_retries} attempts, giving up: {str(e)}")
                        raise
                except Exception as e:
                    logger.error(f"Non-timeout error in {func.__name__}: {str(e)}")
                    raise
            return None
        return wrapper
    return decorator

@retry_on_timeout(max_retries=3, delay=30)
def get_user_playlists(sp, env_path='.env', username=None):
    playlists = sp.user_playlists(username)
    playlist_objs = []

    while playlists:
        for i, playlist in enumerate(playlists['items']):
            print(f"{i + 1 + playlists['offset']:4d} {playlist['uri']} {playlist['name']}")
            playlist_objs.append(playlist)
        if playlists['next']:
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
        for item in tracks['items']:
            if item['track']:
                track_objs.append(item['track'])
        if tracks['next']:
            tracks = sp.next(tracks)
        else:
            tracks = None

    return track_objs

@retry_on_timeout(max_retries=3, delay=30)
def get_artist_info(sp, artist_id):
    artist = sp.artist(artist_id)
    return artist

@retry_on_timeout(max_retries=3, delay=30)
def validate_playlist_id(sp, playlist_id):
    try:
        if not playlist_id or not isinstance(playlist_id, str):
            return create_validation_response(
                error='Invalid playlist ID format: ID must be a non-empty string',
                data_key='playlist_info',
                data_value=None
            )
        
        if playlist_id.startswith('spotify:playlist:'):
            playlist_id = playlist_id.split(':')[-1]
        elif playlist_id.startswith('https://open.spotify.com/playlist/'):
            playlist_id = playlist_id.split('/')[-1].split('?')[0]
        
        playlist = sp.playlist(playlist_id, fields='id,name,public,owner.id,collaborative')
        
        return create_validation_response(
            valid=True,
            accessible=True,
            data_key='playlist_info',
            data_value=playlist
        )
        
    except spotipy.exceptions.SpotifyException as e:
        error_response = handle_spotify_exception(e, "playlist")
        error_response['playlist_info'] = None
        return error_response
    except Exception as e:
        return create_validation_response(
            error=f'Unexpected error: {str(e)}',
            data_key='playlist_info',
            data_value=None
        )

@retry_on_timeout(max_retries=3, delay=30)
def validate_user_id(sp, user_id):
    try:
        if not user_id or not isinstance(user_id, str):
            return create_validation_response(
                error='Invalid user ID format: ID must be a non-empty string',
                data_key='user_info',
                data_value=None
            )
        
        user_id = user_id.strip()
        
        if not user_id:
            return create_validation_response(
                error='User ID cannot be empty or whitespace only',
                data_key='user_info',
                data_value=None
            )
        
        user = sp.user(user_id)
        
        return create_validation_response(
            valid=True,
            accessible=True,
            data_key='user_info',
            data_value=user
        )
        
    except spotipy.exceptions.SpotifyException as e:
        error_response = handle_spotify_exception(e, "user")
        error_response['user_info'] = None
        return error_response
    except Exception as e:
        return create_validation_response(
            error=f'Unexpected error: {str(e)}',
            data_key='user_info',
            data_value=None
        )

@retry_on_timeout(max_retries=3, delay=30)
def validate_user_playlists_accessible(sp, user_id):
    try:
        playlists = sp.user_playlists(user_id, limit=1)
        return create_accessibility_response(
            accessible=True,
            playlist_count=playlists.get('total', 0)
        )
    except spotipy.exceptions.SpotifyException as e:
        if e.http_status == 404:
            return create_accessibility_response(
                error='User not found',
                playlist_count=None
            )
        elif e.http_status == 403:
            return create_accessibility_response(
                error='User playlists are private or not accessible',
                playlist_count=None
            )
        else:
            return create_accessibility_response(
                error=f'Error accessing user playlists: {str(e)}',
                playlist_count=None
            )
    except Exception as e:
        return create_accessibility_response(
            error=f'Unexpected error: {str(e)}',
            playlist_count=None
        )