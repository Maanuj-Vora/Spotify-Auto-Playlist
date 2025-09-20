import sqlite3
from contextlib import contextmanager

DB_PATH = "databases/spotify.db"

_session_connection = None

@contextmanager
def get_connection(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_session(db_path=DB_PATH):
    global _session_connection
    if _session_connection is None:
        _session_connection = sqlite3.connect(db_path)
        _session_connection.row_factory = sqlite3.Row
    return _session_connection

def get_session_connection():
    return _session_connection

def close_session():
    global _session_connection
    if _session_connection is not None:
        _session_connection.close()
        _session_connection = None

@contextmanager
def session_context(db_path=DB_PATH):
    try:
        init_session(db_path)
        yield _session_connection
    finally:
        close_session()

def execute_query(query, params=None, db_path=DB_PATH, use_session=False):
    if use_session and _session_connection is not None:
        cursor = _session_connection.cursor()
        cursor.execute(query, params or [])
        _session_connection.commit()
        return cursor
    else:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or [])
            conn.commit()
            return cursor

def fetch_all(query, params=None, db_path=DB_PATH, use_session=False):
    if use_session and _session_connection is not None:
        cursor = _session_connection.cursor()
        cursor.execute(query, params or [])
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    else:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or [])
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

def fetch_one(query, params=None, db_path=DB_PATH, use_session=False):
    if use_session and _session_connection is not None:
        cursor = _session_connection.cursor()
        cursor.execute(query, params or [])
        row = cursor.fetchone()
        return dict(row) if row else None
    else:
        with get_connection(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or [])
            row = cursor.fetchone()
            return dict(row) if row else None

def _execute_query_auto(query, params=None, db_path=DB_PATH):
    return execute_query(query, params, db_path, use_session=(_session_connection is not None))

def _fetch_all_auto(query, params=None, db_path=DB_PATH):
    return fetch_all(query, params, db_path, use_session=(_session_connection is not None))

def _fetch_one_auto(query, params=None, db_path=DB_PATH):
    return fetch_one(query, params, db_path, use_session=(_session_connection is not None))

def _auto_session_kwargs():
    return {"use_session": _session_connection is not None}
    
def create_tables(db_path=DB_PATH):
    use_session = _session_connection is not None
    create_playlists_table(db_path=db_path, use_session=use_session)
    create_queue_table(db_path=db_path, use_session=use_session)
    create_action_log_table(db_path=db_path, use_session=use_session)
    create_artists_table(db_path=db_path, use_session=use_session)
    create_songs_table(db_path=db_path, use_session=use_session)
    create_playlist_songs_table(db_path=db_path, use_session=use_session)
    create_song_artists_table(db_path=db_path, use_session=use_session)
    
# Playlists fields
# id (TEXT PRIMARY KEY), name (TEXT), description (TEXT), owner_id (TEXT), snapshot_id (TEXT), public (INTEGER), collaborative (INTEGER), tracks_total (INTEGER), href (TEXT), uri (TEXT)

def create_playlists_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS playlists (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        owner_id TEXT,
        snapshot_id TEXT,
        public INTEGER,
        collaborative INTEGER,
        tracks_total INTEGER,
        href TEXT,
        uri TEXT
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Playlist changes fields
# id (INTEGER PRIMARY KEY AUTOINCREMENT), playlist_id (TEXT), playlist_name (TEXT), change_type (TEXT), old_snapshot_id (TEXT), new_snapshot_id (TEXT), detected_at (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

def create_queue_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        playlist_id TEXT NOT NULL,
        playlist_name TEXT,
        change_type TEXT NOT NULL,
        old_snapshot_id TEXT,
        new_snapshot_id TEXT,
        detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (playlist_id) REFERENCES playlists (id)
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Action Log fields
# id (INTEGER PRIMARY KEY AUTOINCREMENT), action_type (TEXT), entity_type (TEXT), entity_id (TEXT), entity_name (TEXT), 
# reason (TEXT), details (TEXT), success (INTEGER), error_message (TEXT), timestamp (TIMESTAMP DEFAULT CURRENT_TIMESTAMP)

def create_action_log_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS action_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        action_type TEXT NOT NULL,
        entity_type TEXT NOT NULL,
        entity_id TEXT,
        entity_name TEXT,
        reason TEXT NOT NULL,
        details TEXT,
        success INTEGER NOT NULL DEFAULT 0,
        error_message TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Artists fields
# id (TEXT PRIMARY KEY), name (TEXT), genres (TEXT), popularity (INTEGER), followers_total (INTEGER), href (TEXT), uri (TEXT), external_urls (TEXT)

def create_artists_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS artists (
        id TEXT PRIMARY KEY,
        name TEXT,
        genres TEXT,
        popularity INTEGER,
        followers_total INTEGER,
        href TEXT,
        uri TEXT,
        external_urls TEXT
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Songs fields
# id (TEXT PRIMARY KEY), name (TEXT), duration_ms (INTEGER), explicit (INTEGER), popularity (INTEGER), preview_url (TEXT), href (TEXT), uri (TEXT), external_urls (TEXT)

def create_songs_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS songs (
        id TEXT PRIMARY KEY,
        name TEXT,
        duration_ms INTEGER,
        explicit INTEGER,
        popularity INTEGER,
        preview_url TEXT,
        href TEXT,
        uri TEXT,
        external_urls TEXT,
        album_id TEXT,
        album_name TEXT
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Playlist Songs junction table (many-to-many relationship)
# playlist_id (TEXT), song_id (TEXT)

def create_playlist_songs_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS playlist_songs (
        playlist_id TEXT NOT NULL,
        song_id TEXT NOT NULL,
        PRIMARY KEY (playlist_id, song_id),
        FOREIGN KEY (playlist_id) REFERENCES playlists (id),
        FOREIGN KEY (song_id) REFERENCES songs (id)
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# Song Artists junction table (many-to-many relationship)
# song_id (TEXT), artist_id (TEXT)

def create_song_artists_table(db_path=DB_PATH, use_session=False):
    query = """
    CREATE TABLE IF NOT EXISTS song_artists (
        song_id TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        PRIMARY KEY (song_id, artist_id),
        FOREIGN KEY (song_id) REFERENCES songs (id),
        FOREIGN KEY (artist_id) REFERENCES artists (id)
    )
    """
    execute_query(query, db_path=db_path, use_session=use_session)

# --- Playlists Functions ---

def insert_playlist(playlist, db_path=DB_PATH):
    query = """
    INSERT OR REPLACE INTO playlists (
        id, name, description, owner_id, snapshot_id, public, collaborative, tracks_total, href, uri
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        playlist.get("id"),
        playlist.get("name"),
        playlist.get("description"),
        playlist.get("owner", {}).get("id"),
        playlist.get("snapshot_id"),
        int(playlist.get("public", False)),
        int(playlist.get("collaborative", False)),
        playlist.get("tracks", {}).get("total"),
        playlist.get("href"),
        playlist.get("uri"),
    )
    _execute_query_auto(query, params, db_path=db_path)

def get_playlist_by_id(playlist_id, db_path=DB_PATH):
    query = "SELECT * FROM playlists WHERE id = ?"
    return _fetch_one_auto(query, [playlist_id], db_path=db_path)

def get_all_playlists(db_path=DB_PATH):
    query = "SELECT * FROM playlists"
    return _fetch_all_auto(query, db_path=db_path)

def delete_playlist(playlist_id, db_path=DB_PATH):
    query = "DELETE FROM playlists WHERE id = ?"
    _execute_query_auto(query, [playlist_id], db_path=db_path)

def check_playlist_modified(playlist, db_path=DB_PATH):
    stored_playlist = get_playlist_by_id(playlist.get("id"), db_path=db_path)
    
    if not stored_playlist:
        return True
    
    stored_snapshot_id = stored_playlist["snapshot_id"]
    current_snapshot_id = playlist.get("snapshot_id")
    
    return stored_snapshot_id != current_snapshot_id

def get_modified_playlists(current_playlists, db_path=DB_PATH):
    modified_playlists = []
    
    for playlist in current_playlists:
        if check_playlist_modified(playlist, db_path=db_path):
            modified_playlists.append(playlist)
    
    return modified_playlists

def get_playlist_snapshot_id(playlist_id, db_path=DB_PATH):
    playlist = get_playlist_by_id(playlist_id, db_path=db_path)
    return playlist["snapshot_id"] if playlist else None

# --- Playlist Changes Functions ---

def insert_playlist_change(playlist_id, playlist_name, change_type, old_snapshot_id=None, new_snapshot_id=None, db_path=DB_PATH):
    query = """
    INSERT INTO queue (
        playlist_id, playlist_name, change_type, old_snapshot_id, new_snapshot_id
    ) VALUES (?, ?, ?, ?, ?)
    """
    params = (playlist_id, playlist_name, change_type, old_snapshot_id, new_snapshot_id)
    execute_query(query, params, db_path=db_path)

def get_queue(limit=None, db_path=DB_PATH):
    query = "SELECT * FROM queue ORDER BY detected_at DESC"
    if limit:
        query += f" LIMIT {limit}"
    return fetch_all(query, db_path=db_path)

def get_queue_by_id(playlist_id, db_path=DB_PATH):
    query = "SELECT * FROM queue WHERE playlist_id = ? ORDER BY detected_at DESC"
    return fetch_all(query, [playlist_id], db_path=db_path)

def delete_queue(playlist_id, db_path=DB_PATH):
    query = "DELETE FROM queue WHERE playlist_id = ?"
    execute_query(query, [playlist_id], db_path=db_path)

def clear_queue(db_path=DB_PATH):
    query = "DELETE FROM queue"
    execute_query(query, db_path=db_path)

# --- Artists Functions ---

def insert_artist(artist, db_path=DB_PATH):
    query = """
    INSERT OR REPLACE INTO artists (
        id, name, genres, popularity, followers_total, href, uri, external_urls
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    genres_str = ", ".join(artist.get("genres", [])) if artist.get("genres") else None
    external_urls_str = str(artist.get("external_urls", {})) if artist.get("external_urls") else None
    
    params = (
        artist.get("id"),
        artist.get("name"),
        genres_str,
        artist.get("popularity"),
        artist.get("followers", {}).get("total"),
        artist.get("href"),
        artist.get("uri"),
        external_urls_str,
    )
    execute_query(query, params, db_path=db_path)

def get_artist_by_id(artist_id, db_path=DB_PATH):
    query = "SELECT * FROM artists WHERE id = ?"
    return fetch_one(query, [artist_id], db_path=db_path)

def get_all_artists(db_path=DB_PATH):
    query = "SELECT * FROM artists"
    return fetch_all(query, db_path=db_path)

def delete_artist(artist_id, db_path=DB_PATH):
    query = "DELETE FROM artists WHERE id = ?"
    execute_query(query, [artist_id], db_path=db_path)

# --- Songs Functions ---

def insert_song(track, db_path=DB_PATH):
    query = """
    INSERT OR REPLACE INTO songs (
        id, name, duration_ms, explicit, popularity, preview_url, href, uri, external_urls, album_id, album_name
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    external_urls_str = str(track.get("external_urls", {})) if track.get("external_urls") else None
    
    params = (
        track.get("id"),
        track.get("name"),
        track.get("duration_ms"),
        int(track.get("explicit", False)),
        track.get("popularity"),
        track.get("preview_url"),
        track.get("href"),
        track.get("uri"),
        external_urls_str,
        track.get("album", {}).get("id"),
        track.get("album", {}).get("name"),
    )
    execute_query(query, params, db_path=db_path)

def get_song_by_id(song_id, db_path=DB_PATH):
    query = "SELECT * FROM songs WHERE id = ?"
    return fetch_one(query, [song_id], db_path=db_path)

def get_all_songs(db_path=DB_PATH):
    query = "SELECT * FROM songs"
    return fetch_all(query, db_path=db_path)

def delete_song(song_id, db_path=DB_PATH):
    query = "DELETE FROM songs WHERE id = ?"
    execute_query(query, [song_id], db_path=db_path)

# --- Playlist Songs Junction Functions ---

def insert_playlist_song(playlist_id, song_id, db_path=DB_PATH):
    query = """
    INSERT OR REPLACE INTO playlist_songs (
        playlist_id, song_id
    ) VALUES (?, ?)
    """
    params = (playlist_id, song_id)
    execute_query(query, params, db_path=db_path)

def get_songs_in_playlist(playlist_id, db_path=DB_PATH):
    query = """
    SELECT s.* 
    FROM songs s 
    JOIN playlist_songs ps ON s.id = ps.song_id 
    WHERE ps.playlist_id = ?
    """
    return fetch_all(query, [playlist_id], db_path=db_path)

def get_playlists_with_song(song_id, db_path=DB_PATH):
    query = """
    SELECT p.* 
    FROM playlists p 
    JOIN playlist_songs ps ON p.id = ps.playlist_id 
    WHERE ps.song_id = ?
    """
    return fetch_all(query, [song_id], db_path=db_path)

def delete_playlist_song(playlist_id, song_id, db_path=DB_PATH):
    query = "DELETE FROM playlist_songs WHERE playlist_id = ? AND song_id = ?"
    execute_query(query, [playlist_id, song_id], db_path=db_path)

def song_exists_in_playlist(playlist_id, song_id, db_path=DB_PATH):
    query = "SELECT 1 FROM playlist_songs WHERE playlist_id = ? AND song_id = ?"
    result = fetch_one(query, [playlist_id, song_id], db_path=db_path)
    return result is not None

def add_song_to_playlist_if_not_exists(playlist_id, song_id, db_path=DB_PATH):
    if not song_exists_in_playlist(playlist_id, song_id, db_path):
        insert_playlist_song(playlist_id, song_id, db_path)
        return True
    return False

def sync_playlist_songs(playlist_id, current_song_ids, db_path=DB_PATH):
    stored_songs = get_songs_in_playlist(playlist_id, db_path)
    stored_song_ids = {song['id'] for song in stored_songs}
    current_song_ids_set = set(current_song_ids)
    
    songs_to_add = current_song_ids_set - stored_song_ids
    
    songs_to_remove = stored_song_ids - current_song_ids_set
    
    added_count = 0
    for song_id in songs_to_add:
        insert_playlist_song(playlist_id, song_id, db_path)
        added_count += 1
    
    removed_count = 0
    for song_id in songs_to_remove:
        delete_playlist_song(playlist_id, song_id, db_path)
        removed_count += 1
    
    return {
        'added': added_count,
        'removed': removed_count,
        'songs_added': list(songs_to_add),
        'songs_removed': list(songs_to_remove)
    }

# --- Song Artists Junction Functions ---

def insert_song_artist(song_id, artist_id, db_path=DB_PATH):
    query = """
    INSERT OR REPLACE INTO song_artists (
        song_id, artist_id
    ) VALUES (?, ?)
    """
    params = (song_id, artist_id)
    execute_query(query, params, db_path=db_path)

def get_artists_for_song(song_id, db_path=DB_PATH):
    query = """
    SELECT a.* 
    FROM artists a 
    JOIN song_artists sa ON a.id = sa.artist_id 
    WHERE sa.song_id = ?
    """
    return fetch_all(query, [song_id], db_path=db_path)

def get_songs_by_artist(artist_id, db_path=DB_PATH):
    query = """
    SELECT s.* 
    FROM songs s 
    JOIN song_artists sa ON s.id = sa.song_id 
    WHERE sa.artist_id = ?
    """
    return fetch_all(query, [artist_id], db_path=db_path)

def delete_song_artist(song_id, artist_id, db_path=DB_PATH):
    query = "DELETE FROM song_artists WHERE song_id = ? AND artist_id = ?"
    execute_query(query, [song_id, artist_id], db_path=db_path)


# --- Orphan Cleanup Functions ---

def get_orphaned_playlists(currently_tracked_playlist_ids, db_path=DB_PATH):
    if not currently_tracked_playlist_ids:
        query = "SELECT * FROM playlists"
        return fetch_all(query, db_path=db_path)
    placeholders = ','.join(['?' for _ in currently_tracked_playlist_ids])
    query = f"""
    SELECT * FROM playlists 
    WHERE id NOT IN ({placeholders})
    """
    return fetch_all(query, currently_tracked_playlist_ids, db_path=db_path)

def delete_playlist_and_relationships(playlist_id, db_path=DB_PATH):

    execute_query("DELETE FROM playlist_songs WHERE playlist_id = ?", [playlist_id], db_path=db_path)
    execute_query("DELETE FROM queue WHERE playlist_id = ?", [playlist_id], db_path=db_path)
    execute_query("DELETE FROM playlists WHERE id = ?", [playlist_id], db_path=db_path)
    return True

def delete_orphaned_playlist_songs(db_path=DB_PATH):
    query = """
    DELETE FROM playlist_songs 
    WHERE playlist_id NOT IN (SELECT id FROM playlists)
    """
    cursor = execute_query(query, db_path=db_path)
    return cursor.rowcount

def delete_orphaned_song_artists(db_path=DB_PATH):
    query = """
    DELETE FROM song_artists 
    WHERE song_id NOT IN (SELECT id FROM songs)
    """
    cursor = execute_query(query, db_path=db_path)
    return cursor.rowcount

def get_orphaned_songs(db_path=DB_PATH):
    query = """
    SELECT s.* 
    FROM songs s 
    LEFT JOIN playlist_songs ps ON s.id = ps.song_id 
    WHERE ps.song_id IS NULL
    """
    return fetch_all(query, db_path=db_path)

def get_orphaned_artists(db_path=DB_PATH):
    query = """
    SELECT a.* 
    FROM artists a 
    LEFT JOIN song_artists sa ON a.id = sa.artist_id 
    WHERE sa.artist_id IS NULL
    """
    return fetch_all(query, db_path=db_path)

# --- Action Log Functions ---

def log_action(action_type, entity_type, entity_id, entity_name, reason, details=None, success=True, error_message=None, db_path=DB_PATH):
    query = """
    INSERT INTO action_log (
        action_type, entity_type, entity_id, entity_name, reason, details, success, error_message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        action_type,
        entity_type,
        entity_id,
        entity_name,
        reason,
        details,
        int(success),
        error_message
    )
    execute_query(query, params, db_path=db_path)

def get_action_logs(limit=None, action_type=None, entity_type=None, entity_id=None, success=None, db_path=DB_PATH):
    query = "SELECT * FROM action_log WHERE 1=1"
    params = []
    
    if action_type:
        query += " AND action_type = ?"
        params.append(action_type)
    
    if entity_type:
        query += " AND entity_type = ?"
        params.append(entity_type)
    
    if entity_id:
        query += " AND entity_id = ?"
        params.append(entity_id)
    
    if success is not None:
        query += " AND success = ?"
        params.append(int(success))
    
    query += " ORDER BY timestamp DESC"
    
    if limit:
        query += f" LIMIT {limit}"
    
    return fetch_all(query, params, db_path=db_path)

def get_recent_action_logs(hours=24, db_path=DB_PATH):
    query = """
    SELECT * FROM action_log 
    WHERE timestamp >= datetime('now', '-{} hours')
    ORDER BY timestamp DESC
    """.format(hours)
    return fetch_all(query, db_path=db_path)

def clear_old_action_logs(days_to_keep=30, db_path=DB_PATH):
    query = """
    DELETE FROM action_log 
    WHERE timestamp < datetime('now', '-{} days')
    """.format(days_to_keep)
    cursor = execute_query(query, db_path=db_path)
    return cursor.rowcount

def get_action_log_summary(db_path=DB_PATH):
    query = """
    SELECT 
        action_type,
        entity_type,
        success,
        COUNT(*) as count,
        MAX(timestamp) as last_occurrence
    FROM action_log 
    GROUP BY action_type, entity_type, success
    ORDER BY action_type, entity_type, success
    """
    return fetch_all(query, db_path=db_path)