import yaml
import os
from utils.logger import Logger

logger = Logger().get_logger()

CONFIG_FILE = "config.yml"
_config_cache = None

def _load_config(config_file=CONFIG_FILE):
    if not os.path.exists(config_file):
        logger.error(f"Configuration file '{config_file}' not found")
        raise FileNotFoundError(f"Configuration file '{config_file}' not found")
    
    try:
        with open(config_file, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
            logger.info(f"Successfully loaded configuration from '{config_file}'")
            return config or {}
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading configuration file: {e}")
        raise

def get_config(config_file=CONFIG_FILE):
    global _config_cache
    if _config_cache is None:
        _config_cache = _load_config(config_file)
    return _config_cache

def reload_config(config_file=CONFIG_FILE):
    global _config_cache
    logger.info("Reloading configuration...")
    _config_cache = _load_config(config_file)
    return _config_cache

def get_usernames(config_file=CONFIG_FILE):
    config = get_config(config_file)
    usernames = config.get('usernames', [])
    if not usernames:
        logger.warning("No usernames found in configuration")
        return []
    
    usernames = [str(username).strip() for username in usernames if username is not None]
    logger.info(f"Found {len(usernames)} usernames in configuration")
    return usernames

def get_playlists_to_track(config_file=CONFIG_FILE):
    config = get_config(config_file)
    playlists = config.get('playlists_to_track', [])
    if not playlists:
        logger.warning("No playlists to track found in configuration")
        return []
    
    playlists = [str(playlist).strip() for playlist in playlists if playlist is not None]
    logger.info(f"Found {len(playlists)} playlists to track in configuration")
    return playlists

def validate_config(config_file=CONFIG_FILE):
    usernames = get_usernames(config_file)
    playlists = get_playlists_to_track(config_file)
    
    if not usernames and not playlists:
        logger.error("Configuration must contain either usernames or playlists_to_track")
        return False
    
    logger.info("Configuration validation passed")
    return True