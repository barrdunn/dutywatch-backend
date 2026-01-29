# db_multiuser.py
"""
Multi-user database extensions for DutyWatch
Add this to your existing db.py or import from it
"""

from __future__ import annotations
import os
import json
import sqlite3
import datetime as dt
import hashlib
import secrets
from typing import Any, Dict, List, Optional
from cryptography.fernet import Fernet

# ---- Encryption Setup ----
# Generate this ONCE and store in .env: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
ENCRYPTION_KEY = os.getenv("DUTYWATCH_ENCRYPTION_KEY", "")

def _get_cipher():
    """Get Fernet cipher for encrypting/decrypting passwords."""
    if not ENCRYPTION_KEY:
        raise RuntimeError("DUTYWATCH_ENCRYPTION_KEY not set in .env")
    return Fernet(ENCRYPTION_KEY.encode())

def encrypt_password(password: str) -> str:
    """Encrypt a password for storage."""
    cipher = _get_cipher()
    return cipher.encrypt(password.encode()).decode()

def decrypt_password(encrypted: str) -> str:
    """Decrypt a stored password."""
    cipher = _get_cipher()
    return cipher.decrypt(encrypted.encode()).decode()


# ---- Multi-User Schema ----

def init_multiuser_tables(conn: sqlite3.Connection) -> None:
    """Create multi-user tables. Call this from init_db()."""
    conn.executescript("""
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            display_name TEXT,
            icloud_user TEXT,
            icloud_app_pw_encrypted TEXT,
            caldav_url TEXT DEFAULT 'https://caldav.icloud.com/',
            calendar_name TEXT,
            timezone TEXT DEFAULT 'America/Chicago',
            home_base TEXT DEFAULT 'DFW',
            created_at TEXT,
            last_login_at TEXT,
            is_active INTEGER DEFAULT 1
        );
        
        -- User sessions (optional - for cookie-based auth)
        CREATE TABLE IF NOT EXISTS user_sessions (
            session_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TEXT,
            expires_at TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user events cache
        CREATE TABLE IF NOT EXISTS user_events_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            scope TEXT DEFAULT 'default',
            uid_hash TEXT,
            json TEXT,
            updated_at TEXT,
            UNIQUE(user_id, scope),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user hidden items
        CREATE TABLE IF NOT EXISTS user_hidden_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pairing_id TEXT NOT NULL,
            report_local_iso TEXT,
            created_at TEXT,
            UNIQUE(user_id, pairing_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user profile
        CREATE TABLE IF NOT EXISTS user_profile (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            photo TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user commute prefs
        CREATE TABLE IF NOT EXISTS user_commute_prefs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pairing_id TEXT NOT NULL,
            report_local_iso TEXT,
            tracking_url TEXT,
            last_updated TEXT,
            UNIQUE(user_id, pairing_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user acks
        CREATE TABLE IF NOT EXISTS user_acks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            ack_id TEXT NOT NULL,
            event_uid TEXT,
            deadline_utc TEXT,
            state TEXT,
            last_update_utc TEXT,
            UNIQUE(user_id, ack_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
        CREATE INDEX IF NOT EXISTS idx_user_events_user ON user_events_cache(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_hidden_user ON user_hidden_items(user_id);
    """)


# ---- User CRUD ----

def create_user(
    conn: sqlite3.Connection,
    username: str,
    display_name: str = None,
    icloud_user: str = None,
    icloud_app_pw: str = None,
    calendar_name: str = None,
    timezone: str = "America/Chicago",
    home_base: str = "DFW"
) -> int:
    """Create a new user. Returns user_id."""
    now = dt.datetime.utcnow().isoformat()
    
    # Validate username (alphanumeric, underscore, hyphen only)
    import re
    if not re.match(r'^[a-zA-Z0-9_-]{3,30}$', username):
        raise ValueError("Username must be 3-30 characters, alphanumeric, underscore or hyphen only")
    
    # Encrypt password if provided
    encrypted_pw = encrypt_password(icloud_app_pw) if icloud_app_pw else None
    
    cursor = conn.execute("""
        INSERT INTO users (username, display_name, icloud_user, icloud_app_pw_encrypted, 
                          calendar_name, timezone, home_base, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (username.lower(), display_name or username, icloud_user, encrypted_pw,
          calendar_name, timezone, home_base, now))
    
    user_id = cursor.lastrowid
    
    # Create profile entry
    conn.execute("""
        INSERT INTO user_profile (user_id, first_name, last_name)
        VALUES (?, ?, ?)
    """, (user_id, display_name or username, ""))
    
    return user_id


def get_user_by_username(conn: sqlite3.Connection, username: str) -> Optional[Dict[str, Any]]:
    """Get user by username."""
    row = conn.execute(
        "SELECT * FROM users WHERE username = ? AND is_active = 1",
        (username.lower(),)
    ).fetchone()
    return dict(row) if row else None


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    """Get user by ID."""
    row = conn.execute(
        "SELECT * FROM users WHERE id = ? AND is_active = 1",
        (user_id,)
    ).fetchone()
    return dict(row) if row else None


def update_user_credentials(
    conn: sqlite3.Connection,
    user_id: int,
    icloud_user: str = None,
    icloud_app_pw: str = None,
    calendar_name: str = None
) -> bool:
    """Update user's iCloud credentials."""
    updates = []
    params = []
    
    if icloud_user is not None:
        updates.append("icloud_user = ?")
        params.append(icloud_user)
    
    if icloud_app_pw is not None:
        updates.append("icloud_app_pw_encrypted = ?")
        params.append(encrypt_password(icloud_app_pw) if icloud_app_pw else None)
    
    if calendar_name is not None:
        updates.append("calendar_name = ?")
        params.append(calendar_name)
    
    if not updates:
        return False
    
    params.append(user_id)
    conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params)
    return True


def get_user_decrypted_credentials(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, str]]:
    """Get user's decrypted iCloud credentials for calendar fetching."""
    row = conn.execute(
        "SELECT icloud_user, icloud_app_pw_encrypted, caldav_url, calendar_name FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()
    
    if not row:
        return None
    
    return {
        "icloud_user": row["icloud_user"],
        "icloud_app_pw": decrypt_password(row["icloud_app_pw_encrypted"]) if row["icloud_app_pw_encrypted"] else None,
        "caldav_url": row["caldav_url"],
        "calendar_name": row["calendar_name"]
    }


def list_all_users(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """List all active users (for admin)."""
    rows = conn.execute(
        "SELECT id, username, display_name, calendar_name, home_base, created_at FROM users WHERE is_active = 1"
    ).fetchall()
    return [dict(r) for r in rows]


# ---- Per-User Events Cache ----

def get_user_events_cache(conn: sqlite3.Connection, user_id: int, scope: str = "default") -> List[Dict[str, Any]]:
    """Get cached events for a user."""
    row = conn.execute(
        "SELECT json FROM user_events_cache WHERE user_id = ? AND scope = ?",
        (user_id, scope)
    ).fetchone()
    if not row or not row["json"]:
        return []
    try:
        return json.loads(row["json"])
    except:
        return []


def set_user_events_cache(conn: sqlite3.Connection, user_id: int, events: List[Dict[str, Any]], 
                          scope: str = "default", uid_hash: str = None) -> None:
    """Set cached events for a user."""
    now = dt.datetime.utcnow().isoformat()
    payload = json.dumps(events, ensure_ascii=False)
    
    conn.execute("""
        INSERT INTO user_events_cache (user_id, scope, uid_hash, json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id, scope) DO UPDATE SET 
            uid_hash = excluded.uid_hash,
            json = excluded.json,
            updated_at = excluded.updated_at
    """, (user_id, scope, uid_hash, payload, now))


# ---- Per-User Hidden Items ----

def user_hidden_add(conn: sqlite3.Connection, user_id: int, pairing_id: str, report_local_iso: str = None) -> None:
    """Hide a pairing for a specific user."""
    now = dt.datetime.utcnow().isoformat()
    conn.execute("""
        INSERT INTO user_hidden_items (user_id, pairing_id, report_local_iso, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, pairing_id) DO UPDATE SET report_local_iso = excluded.report_local_iso
    """, (user_id, pairing_id, report_local_iso or "", now))


def user_hidden_list(conn: sqlite3.Connection, user_id: int) -> List[str]:
    """List hidden pairing IDs for a user."""
    rows = conn.execute(
        "SELECT pairing_id FROM user_hidden_items WHERE user_id = ?",
        (user_id,)
    ).fetchall()
    return [r["pairing_id"] for r in rows]


def user_hidden_clear(conn: sqlite3.Connection, user_id: int) -> int:
    """Clear all hidden items for a user. Returns count cleared."""
    cursor = conn.execute("DELETE FROM user_hidden_items WHERE user_id = ?", (user_id,))
    return cursor.rowcount


def user_hidden_count(conn: sqlite3.Connection, user_id: int) -> int:
    """Count hidden items for a user."""
    row = conn.execute(
        "SELECT COUNT(*) as n FROM user_hidden_items WHERE user_id = ?",
        (user_id,)
    ).fetchone()
    return row["n"] if row else 0


# ---- Per-User Profile ----

def get_user_profile(conn: sqlite3.Connection, user_id: int) -> Dict[str, Any]:
    """Get user profile."""
    row = conn.execute(
        "SELECT first_name, last_name, photo FROM user_profile WHERE user_id = ?",
        (user_id,)
    ).fetchone()
    if row:
        return {
            "firstName": row["first_name"] or "",
            "lastName": row["last_name"] or "",
            "photo": row["photo"]
        }
    return {"firstName": "", "lastName": "", "photo": None}


def save_user_profile(conn: sqlite3.Connection, user_id: int, first_name: str, last_name: str, photo: str = None) -> None:
    """Save user profile."""
    conn.execute("""
        INSERT INTO user_profile (user_id, first_name, last_name, photo)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            photo = excluded.photo
    """, (user_id, first_name, last_name, photo))

    # ============================================================================
# MULTI-USER SUPPORT - ADD THIS TO THE END OF YOUR db.py
# ============================================================================

from cryptography.fernet import Fernet
import re

# ---- Encryption Setup ----
# Generate key ONCE: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Store in environment variable or systemd service file
ENCRYPTION_KEY = os.getenv("DUTYWATCH_ENCRYPTION_KEY", "")

def _get_cipher():
    """Get Fernet cipher for encrypting/decrypting passwords."""
    if not ENCRYPTION_KEY:
        raise RuntimeError("DUTYWATCH_ENCRYPTION_KEY not set")
    return Fernet(ENCRYPTION_KEY.encode())

def encrypt_password(password: str) -> str:
    """Encrypt a password for storage."""
    cipher = _get_cipher()
    return cipher.encrypt(password.encode()).decode()

def decrypt_password(encrypted: str) -> str:
    """Decrypt a stored password."""
    cipher = _get_cipher()
    return cipher.decrypt(encrypted.encode()).decode()


# ---- Multi-User Schema ----

def init_multiuser_tables(conn: sqlite3.Connection) -> None:
    """Create multi-user tables. Called from app.py lifespan."""
    conn.executescript("""
        -- Users table
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            display_name TEXT,
            icloud_user TEXT,
            icloud_app_pw_encrypted TEXT,
            caldav_url TEXT DEFAULT 'https://caldav.icloud.com/',
            calendar_name TEXT,
            timezone TEXT DEFAULT 'America/Chicago',
            home_base TEXT DEFAULT 'DFW',
            created_at TEXT,
            last_login_at TEXT,
            is_active INTEGER DEFAULT 1
        );
        
        -- Per-user events cache
        CREATE TABLE IF NOT EXISTS user_events_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            scope TEXT DEFAULT 'default',
            uid_hash TEXT,
            json TEXT,
            updated_at TEXT,
            UNIQUE(user_id, scope),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user hidden items
        CREATE TABLE IF NOT EXISTS user_hidden_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            pairing_id TEXT NOT NULL,
            report_local_iso TEXT,
            created_at TEXT,
            UNIQUE(user_id, pairing_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        -- Per-user profile
        CREATE TABLE IF NOT EXISTS user_profile (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            photo TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
        CREATE INDEX IF NOT EXISTS idx_user_events_user ON user_events_cache(user_id);
        CREATE INDEX IF NOT EXISTS idx_user_hidden_user ON user_hidden_items(user_id);
    """)


# ---- User CRUD ----

def create_user(
    conn: sqlite3.Connection,
    username: str,
    display_name: str = None,
    icloud_user: str = None,
    icloud_app_pw: str = None,
    calendar_name: str = None,
    timezone: str = "America/Chicago",
    home_base: str = "DFW"
) -> int:
    """Create a new user. Returns user_id."""
    now = dt.datetime.utcnow().isoformat()
    
    if not re.match(r'^[a-zA-Z0-9_-]{3,30}$', username):
        raise ValueError("Username must be 3-30 characters, alphanumeric, underscore or hyphen only")
    
    encrypted_pw = encrypt_password(icloud_app_pw) if icloud_app_pw else None
    
    cursor = conn.execute("""
        INSERT INTO users (username, display_name, icloud_user, icloud_app_pw_encrypted, 
                          calendar_name, timezone, home_base, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (username.lower(), display_name or username, icloud_user, encrypted_pw,
          calendar_name, timezone, home_base, now))
    
    user_id = cursor.lastrowid
    
    conn.execute("""
        INSERT INTO user_profile (user_id, first_name, last_name)
        VALUES (?, ?, ?)
    """, (user_id, display_name or username, ""))
    
    return user_id


def get_user_by_username(conn: sqlite3.Connection, username: str) -> Optional[Dict[str, Any]]:
    """Get user by username."""
    row = conn.execute(
        "SELECT * FROM users WHERE username = ? AND is_active = 1",
        (username.lower(),)
    ).fetchone()
    return dict(row) if row else None


def get_user_by_id(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, Any]]:
    """Get user by ID."""
    row = conn.execute(
        "SELECT * FROM users WHERE id = ? AND is_active = 1",
        (user_id,)
    ).fetchone()
    return dict(row) if row else None


def update_user_credentials(
    conn: sqlite3.Connection,
    user_id: int,
    icloud_user: str = None,
    icloud_app_pw: str = None,
    calendar_name: str = None
) -> bool:
    """Update user's iCloud credentials."""
    updates = []
    params = []
    
    if icloud_user is not None:
        updates.append("icloud_user = ?")
        params.append(icloud_user)
    
    if icloud_app_pw is not None:
        updates.append("icloud_app_pw_encrypted = ?")
        params.append(encrypt_password(icloud_app_pw) if icloud_app_pw else None)
    
    if calendar_name is not None:
        updates.append("calendar_name = ?")
        params.append(calendar_name)
    
    if not updates:
        return False
    
    params.append(user_id)
    conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params)
    return True


def get_user_decrypted_credentials(conn: sqlite3.Connection, user_id: int) -> Optional[Dict[str, str]]:
    """Get user's decrypted iCloud credentials."""
    row = conn.execute(
        "SELECT icloud_user, icloud_app_pw_encrypted, caldav_url, calendar_name FROM users WHERE id = ?",
        (user_id,)
    ).fetchone()
    
    if not row:
        return None
    
    return {
        "icloud_user": row["icloud_user"],
        "icloud_app_pw": decrypt_password(row["icloud_app_pw_encrypted"]) if row["icloud_app_pw_encrypted"] else None,
        "caldav_url": row["caldav_url"],
        "calendar_name": row["calendar_name"]
    }


def list_all_users(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """List all active users."""
    rows = conn.execute(
        "SELECT id, username, display_name, calendar_name, home_base, created_at FROM users WHERE is_active = 1"
    ).fetchall()
    return [dict(r) for r in rows]


# ---- Per-User Events Cache ----

def get_user_events_cache(conn: sqlite3.Connection, user_id: int, scope: str = "default") -> List[Dict[str, Any]]:
    """Get cached events for a user."""
    row = conn.execute(
        "SELECT json FROM user_events_cache WHERE user_id = ? AND scope = ?",
        (user_id, scope)
    ).fetchone()
    if not row or not row["json"]:
        return []
    try:
        return json.loads(row["json"])
    except:
        return []


def set_user_events_cache(conn: sqlite3.Connection, user_id: int, events: List[Dict[str, Any]], 
                          scope: str = "default", uid_hash: str = None) -> None:
    """Set cached events for a user."""
    now = dt.datetime.utcnow().isoformat()
    payload = json.dumps(events, ensure_ascii=False)
    
    conn.execute("""
        INSERT INTO user_events_cache (user_id, scope, uid_hash, json, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id, scope) DO UPDATE SET 
            uid_hash = excluded.uid_hash,
            json = excluded.json,
            updated_at = excluded.updated_at
    """, (user_id, scope, uid_hash, payload, now))


# ---- Per-User Hidden Items ----

def user_hidden_add(conn: sqlite3.Connection, user_id: int, pairing_id: str, report_local_iso: str = None) -> None:
    """Hide a pairing for a specific user."""
    now = dt.datetime.utcnow().isoformat()
    conn.execute("""
        INSERT INTO user_hidden_items (user_id, pairing_id, report_local_iso, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, pairing_id) DO UPDATE SET report_local_iso = excluded.report_local_iso
    """, (user_id, pairing_id, report_local_iso or "", now))


def user_hidden_list(conn: sqlite3.Connection, user_id: int) -> List[str]:
    """List hidden pairing IDs for a user."""
    rows = conn.execute(
        "SELECT pairing_id FROM user_hidden_items WHERE user_id = ?",
        (user_id,)
    ).fetchall()
    return [r["pairing_id"] for r in rows]


def user_hidden_clear(conn: sqlite3.Connection, user_id: int) -> int:
    """Clear all hidden items for a user."""
    cursor = conn.execute("DELETE FROM user_hidden_items WHERE user_id = ?", (user_id,))
    return cursor.rowcount


def user_hidden_count(conn: sqlite3.Connection, user_id: int) -> int:
    """Count hidden items for a user."""
    row = conn.execute(
        "SELECT COUNT(*) as n FROM user_hidden_items WHERE user_id = ?",
        (user_id,)
    ).fetchone()
    return row["n"] if row else 0


# ---- Per-User Profile ----

def get_user_profile(conn: sqlite3.Connection, user_id: int) -> Dict[str, Any]:
    """Get user profile."""
    row = conn.execute(
        "SELECT first_name, last_name, photo FROM user_profile WHERE user_id = ?",
        (user_id,)
    ).fetchone()
    if row:
        return {
            "firstName": row["first_name"] or "",
            "lastName": row["last_name"] or "",
            "photo": row["photo"]
        }
    return {"firstName": "", "lastName": "", "photo": None}


def save_user_profile(conn: sqlite3.Connection, user_id: int, first_name: str, last_name: str, photo: str = None) -> None:
    """Save user profile."""
    conn.execute("""
        INSERT INTO user_profile (user_id, first_name, last_name, photo)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET 
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            photo = excluded.photo
    """, (user_id, first_name, last_name, photo))