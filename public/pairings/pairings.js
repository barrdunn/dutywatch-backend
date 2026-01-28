# app_multiuser.py
"""
Multi-user API routes for DutyWatch
Add these routes to your existing app.py

Usage:
1. Import this module in app.py
2. Call register_multiuser_routes(app) after creating the FastAPI app
3. IMPORTANT: Call register_multiuser_routes LAST (after all other routes)
   so that /{username} doesn't catch /api, /pairings, etc.

AWS Deployment Notes:
- Store DUTYWATCH_ENCRYPTION_KEY in AWS Secrets Manager or Parameter Store
- Or set as environment variable in your EC2/ECS/Lambda config
- The SQLite database will be on the EC2 instance's disk (consider RDS for production scale)
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import traceback
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Body, Query, Depends, Request
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from starlette.concurrency import run_in_threadpool
from zoneinfo import ZoneInfo

# Import the multiuser database functions
from db_multiuser import (
    init_multiuser_tables,
    create_user, get_user_by_username, get_user_by_id, update_user_credentials,
    get_user_decrypted_credentials, list_all_users,
    get_user_events_cache, set_user_events_cache,
    user_hidden_add, user_hidden_list, user_hidden_clear, user_hidden_count,
    get_user_profile, save_user_profile
)
from cal_client_multiuser import (
    list_calendars_for_user, diagnose_user_connection, fetch_events_for_user
)

# These imports would come from your existing modules
# from modules.db import get_db
# from modules.rows import build_pairing_rows, end_of_next_month_local
# from modules.cache import normalize_cached_events
# from modules.utils import iso_to_dt, to_local, to_utc

logger = logging.getLogger("dutywatch.multiuser")


def register_multiuser_routes(app: FastAPI, get_db_func, build_rows_func):
    """
    Register all multi-user routes on the FastAPI app.
    
    Args:
        app: FastAPI application instance
        get_db_func: Function to get database connection (from modules.db)
        build_rows_func: Function to build pairing rows (from modules.rows)
    """
    
    # ==========================================
    # User Setup / Registration
    # ==========================================
    
    @app.post("/api/users/register")
    async def register_user(payload: Dict[str, Any] = Body(...)):
        """
        Register a new user.
        
        Expected payload:
        {
            "username": "pilotjohn",
            "display_name": "John Smith",  # optional
            "timezone": "America/Chicago",  # optional
            "home_base": "DFW"  # optional
        }
        """
        username = str(payload.get("username") or "").strip().lower()
        if not username:
            raise HTTPException(400, "username is required")
        
        display_name = payload.get("display_name") or username
        timezone = payload.get("timezone", "America/Chicago")
        home_base = payload.get("home_base", "DFW").upper()
        
        def do_create():
            with get_db_func() as conn:
                # Check if username exists
                existing = get_user_by_username(conn, username)
                if existing:
                    raise HTTPException(409, "Username already exists")
                
                user_id = create_user(
                    conn, username, display_name,
                    timezone=timezone, home_base=home_base
                )
                return user_id
        
        try:
            user_id = await run_in_threadpool(do_create)
            return {
                "ok": True,
                "user_id": user_id,
                "username": username,
                "url": f"/{username}"  # mydutywatch.com/pilotjohn
            }
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(400, str(e))
        except Exception as e:
            logger.exception("Failed to create user")
            raise HTTPException(500, f"Failed to create user: {e}")
    
    
    @app.post("/api/users/{username}/setup-credentials")
    async def setup_user_credentials(username: str, payload: Dict[str, Any] = Body(...)):
        """
        Set up iCloud credentials for a user.
        This is called from the setup page after the user pastes their app-specific password.
        
        Expected payload:
        {
            "icloud_user": "user@icloud.com",
            "icloud_app_pw": "xxxx-xxxx-xxxx-xxxx"
        }
        """
        icloud_user = str(payload.get("icloud_user") or "").strip()
        icloud_app_pw = str(payload.get("icloud_app_pw") or "").strip()
        
        if not icloud_user or not icloud_app_pw:
            raise HTTPException(400, "icloud_user and icloud_app_pw are required")
        
        def do_setup():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                # Test the connection first
                diag = diagnose_user_connection(icloud_user, icloud_app_pw)
                if not diag.get("ok"):
                    raise HTTPException(400, f"iCloud connection failed: {diag.get('error')}")
                
                # Save credentials
                update_user_credentials(conn, user["id"], icloud_user, icloud_app_pw)
                
                return {
                    "ok": True,
                    "calendars": diag.get("calendars", [])
                }
        
        try:
            return await run_in_threadpool(do_setup)
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to setup credentials")
            raise HTTPException(500, f"Setup failed: {e}")
    
    
    @app.get("/api/users/{username}/calendars")
    async def list_user_calendars(username: str):
        """
        List available calendars for a user.
        Called after credentials are set up to let user pick which calendar to use.
        """
        def do_list():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                creds = get_user_decrypted_credentials(conn, user["id"])
                if not creds or not creds.get("icloud_user"):
                    raise HTTPException(400, "Credentials not configured")
                
                calendars = list_calendars_for_user(
                    creds["icloud_user"],
                    creds["icloud_app_pw"],
                    creds.get("caldav_url", "https://caldav.icloud.com/")
                )
                return calendars
        
        try:
            calendars = await run_in_threadpool(do_list)
            return {"calendars": calendars}
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to list calendars")
            raise HTTPException(500, f"Failed to list calendars: {e}")
    
    
    @app.post("/api/users/{username}/select-calendar")
    async def select_user_calendar(username: str, payload: Dict[str, Any] = Body(...)):
        """
        Select which calendar to use for this user.
        
        Expected payload:
        {
            "calendar_name": "Pairings"
        }
        """
        calendar_name = str(payload.get("calendar_name") or "").strip()
        if not calendar_name:
            raise HTTPException(400, "calendar_name is required")
        
        def do_select():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                update_user_credentials(conn, user["id"], calendar_name=calendar_name)
                return True
        
        try:
            await run_in_threadpool(do_select)
            return {"ok": True, "calendar_name": calendar_name}
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to select calendar")
            raise HTTPException(500, f"Failed to select calendar: {e}")
    
    
    # ==========================================
    # Per-User Pairings API
    # ==========================================
    
    @app.get("/api/u/{username}/pairings")
    async def get_user_pairings(
        username: str,
        is_24h: int = Query(default=0),
        only_reports: int = Query(default=1)
    ):
        """
        Get pairings for a specific user.
        This is the per-user equivalent of /api/pairings
        """
        def do_fetch():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                creds = get_user_decrypted_credentials(conn, user["id"])
                if not creds or not creds.get("icloud_user"):
                    raise HTTPException(400, "Credentials not configured. Please complete setup.")
                
                # Calculate date range
                tz = ZoneInfo(user.get("timezone", "America/Chicago"))
                now_local = dt.datetime.now(tz)
                
                # Start from beginning of last month
                if now_local.month == 1:
                    start_month, start_year = 12, now_local.year - 1
                else:
                    start_month, start_year = now_local.month - 1, now_local.year
                
                start_local = dt.datetime(start_year, start_month, 1, tzinfo=tz)
                
                # End at end of next month
                if now_local.month == 12:
                    next_month, next_year = 1, now_local.year + 1
                else:
                    next_month, next_year = now_local.month + 1, now_local.year
                
                if next_month == 12:
                    end_month, end_year = 1, next_year + 1
                else:
                    end_month, end_year = next_month + 1, next_year
                
                end_utc = dt.datetime(end_year, end_month, 1, tzinfo=dt.timezone.utc)
                start_utc = start_local.astimezone(dt.timezone.utc)
                
                # Fetch events
                events = fetch_events_for_user(
                    creds["icloud_user"],
                    creds["icloud_app_pw"],
                    creds.get("caldav_url", "https://caldav.icloud.com/"),
                    creds.get("calendar_name"),
                    start_utc.isoformat(),
                    end_utc.isoformat()
                )
                
                # Cache events
                set_user_events_cache(conn, user["id"], events)
                
                # Get hidden items
                hidden_pids = user_hidden_list(conn, user["id"])
                hidden_count = user_hidden_count(conn, user["id"])
                
                # Get profile
                profile = get_user_profile(conn, user["id"])
                
                return {
                    "user": user,
                    "events": events,
                    "hidden_pids": set(hidden_pids),
                    "hidden_count": hidden_count,
                    "profile": profile,
                    "tz": tz,
                    "home_base": user.get("home_base", "DFW")
                }
        
        try:
            data = await run_in_threadpool(do_fetch)
            
            # Build rows using the existing build_pairing_rows function
            # This would need to be adapted based on your actual implementation
            rows = await run_in_threadpool(
                build_rows_func,
                data["events"],
                bool(is_24h),
                bool(only_reports),
                True,  # include_off_rows
                data["home_base"],
                True,  # filter_past
                False  # include_non_pairing_events
            )
            
            # Filter out hidden
            visible = [r for r in rows if r.get("pairing_id") not in data["hidden_pids"] or r.get("kind") == "off"]
            
            return {
                "rows": visible,
                "calendar_rows": rows,  # For calendar widget
                "profile": data["profile"],
                "hidden_count": data["hidden_count"],
                "tz_label": str(data["tz"]).split("/")[-1],
                "home_base": data["home_base"]
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.exception("Failed to get pairings")
            return JSONResponse(
                status_code=500,
                content={"error": "pairings_failed", "message": str(e)}
            )
    
    
    @app.post("/api/u/{username}/refresh")
    async def refresh_user_pairings(username: str):
        """Force refresh pairings for a user."""
        # Similar to get_user_pairings but forces a fresh fetch
        # For brevity, could just call get_user_pairings
        return await get_user_pairings(username)
    
    
    @app.post("/api/u/{username}/hidden/hide")
    async def hide_user_pairing(username: str, payload: Dict[str, Any] = Body(...)):
        """Hide a pairing for a user."""
        pairing_id = str(payload.get("pairing_id") or "").strip()
        if not pairing_id:
            raise HTTPException(400, "pairing_id required")
        
        def do_hide():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                user_hidden_add(conn, user["id"], pairing_id, payload.get("report_local_iso"))
                return user_hidden_count(conn, user["id"])
        
        count = await run_in_threadpool(do_hide)
        return {"ok": True, "hidden_count": count}
    
    
    @app.post("/api/u/{username}/hidden/unhide_all")
    async def unhide_all_user_pairings(username: str):
        """Unhide all pairings for a user."""
        def do_unhide():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                count = user_hidden_clear(conn, user["id"])
                return count
        
        count = await run_in_threadpool(do_unhide)
        return {"ok": True, "cleared": count}
    
    
    # ==========================================
    # Per-User Profile API
    # ==========================================
    
    @app.get("/api/u/{username}/profile")
    async def get_profile(username: str):
        """Get user profile."""
        def do_get():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                return get_user_profile(conn, user["id"])
        
        return await run_in_threadpool(do_get)
    
    
    @app.post("/api/u/{username}/profile")
    async def update_profile(username: str, payload: Dict[str, Any] = Body(...)):
        """Update user profile."""
        def do_update():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                if not user:
                    raise HTTPException(404, "User not found")
                
                save_user_profile(
                    conn, user["id"],
                    str(payload.get("firstName") or "").strip(),
                    str(payload.get("lastName") or "").strip(),
                    payload.get("photo")
                )
                return True
        
        await run_in_threadpool(do_update)
        return {"ok": True}
    
    
    # ==========================================
    # User Page Routes (HTML)
    # ==========================================
    
    # List of reserved paths that should NOT be treated as usernames
    RESERVED_PATHS = {
        'api', 'pairings', 'medical', 'med_portal', 'health', 'static',
        'favicon.ico', 'robots.txt', 'setup', 'admin', 'login', 'register'
    }
    
    @app.get("/{username}", response_class=HTMLResponse)
    async def user_pairings_page(username: str):
        """
        Serve the pairings page for a specific user.
        URL: mydutywatch.com/pilotjohn
        
        IMPORTANT: This route must be registered LAST to avoid
        catching /api, /pairings, /health, etc.
        """
        # Block reserved paths
        if username.lower() in RESERVED_PATHS:
            raise HTTPException(404, "Not found")
        
        def check_user():
            with get_db_func() as conn:
                user = get_user_by_username(conn, username)
                return user
        
        user = await run_in_threadpool(check_user)
        if not user:
            raise HTTPException(404, "User not found")
        
        # Check if setup is complete
        has_creds = bool(user.get("icloud_user") and user.get("icloud_app_pw_encrypted"))
        has_calendar = bool(user.get("calendar_name"))
        
        if not has_creds or not has_calendar:
            # Redirect to setup page
            from fastapi.responses import RedirectResponse
            return RedirectResponse(url=f"/{username}/setup", status_code=302)
        
        # Serve the pairings page with user config injected
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>DutyWatch - {user.get('display_name', username)}</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <link rel="stylesheet" href="/pairings/pairings.css">
            <link rel="stylesheet" href="/pairings/settings/settings.css">
        </head>
        <body>
            <script>
                // Configure for this specific user
                window.dwConfig = {{
                    apiBase: '/api/u/{username}',
                    username: '{username}',
                    clockMode: '12',
                    baseAirport: '{user.get("home_base", "DFW")}'
                }};
            </script>
            <script>
                // Redirect to pairings page with user context
                window.location.href = '/pairings/?user={username}';
            </script>
            <noscript>
                <meta http-equiv="refresh" content="0;url=/pairings/?user={username}">
            </noscript>
        </body>
        </html>
        """
    
    
    @app.get("/{username}/setup", response_class=HTMLResponse)
    async def user_setup_page(username: str):
        """
        Serve the setup page for a new user.
        URL: mydutywatch.com/pilotjohn/setup
        """
        # Block reserved paths
        if username.lower() in RESERVED_PATHS:
            raise HTTPException(404, "Not found")
        
        def check_user():
            with get_db_func() as conn:
                return get_user_by_username(conn, username)
        
        user = await run_in_threadpool(check_user)
        if not user:
            raise HTTPException(404, "User not found")
        
        # Return setup page HTML (see setup.html file)
        from pathlib import Path
        setup_html = Path(__file__).parent / "public" / "setup" / "setup.html"
        if setup_html.exists():
            return FileResponse(setup_html)
        
        # Fallback inline HTML for setup page
        return f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8">
            <title>Setup - DutyWatch</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                :root {{ color-scheme: dark; --bg: #0b0f14; --card: #121821; --border: #1e2a38; --text: #e8edf2; --accent: #6fb1ff; }}
                * {{ box-sizing: border-box; }}
                body {{ margin: 0; background: var(--bg); color: var(--text); font: 14px/1.5 system-ui, sans-serif; padding: 20px; }}
                .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 24px; max-width: 500px; margin: 40px auto; }}
                h1 {{ margin: 0 0 20px; font-size: 24px; }}
                .field {{ margin-bottom: 16px; }}
                label {{ display: block; margin-bottom: 6px; color: #9fb0c0; }}
                input, select {{ width: 100%; padding: 10px 12px; background: var(--bg); border: 1px solid var(--border); border-radius: 8px; color: var(--text); font-size: 14px; }}
                .btn {{ background: var(--accent); color: #031323; border: 0; border-radius: 10px; padding: 12px 20px; font-weight: 600; cursor: pointer; width: 100%; font-size: 16px; }}
                .btn:disabled {{ opacity: 0.6; cursor: not-allowed; }}
                .help {{ font-size: 12px; color: #9fb0c0; margin-top: 6px; }}
                .help a {{ color: var(--accent); }}
                .error {{ color: #ff6b6b; margin: 12px 0; }}
                .success {{ color: #98f5a7; margin: 12px 0; }}
                .step {{ display: none; }}
                .step.active {{ display: block; }}
                .logo {{ text-align: center; margin-bottom: 20px; font-size: 28px; }}
            </style>
        </head>
        <body>
            <div class="card">
                <div class="logo">✈️ DutyWatch</div>
                <h1>Welcome, {user.get('display_name', username)}!</h1>
                <p>Let's connect your iCloud calendar to DutyWatch.</p>
                
                <div id="step1" class="step active">
                    <div class="field">
                        <label>iCloud Email</label>
                        <input type="email" id="icloud-user" placeholder="your@icloud.com" autocomplete="email">
                    </div>
                    <div class="field">
                        <label>App-Specific Password</label>
                        <input type="text" id="icloud-pw" placeholder="xxxx-xxxx-xxxx-xxxx" autocomplete="off" 
                               style="font-family: monospace; letter-spacing: 1px;">
                        <div class="help">
                            Create one at <a href="https://appleid.apple.com/account/manage" target="_blank">appleid.apple.com</a><br>
                            Sign-In and Security → App-Specific Passwords → Generate
                        </div>
                    </div>
                    <div id="step1-error" class="error" style="display:none"></div>
                    <button class="btn" id="connect-btn">Connect to iCloud</button>
                </div>
                
                <div id="step2" class="step">
                    <div class="success">✓ Connected to iCloud!</div>
                    <div class="field">
                        <label>Select Calendar</label>
                        <select id="calendar-select">
                            <option value="">Loading calendars...</option>
                        </select>
                        <div class="help">Choose the calendar containing your flight pairings</div>
                    </div>
                    <div id="step2-error" class="error" style="display:none"></div>
                    <button class="btn" id="finish-btn">Finish Setup</button>
                </div>
                
                <div id="step3" class="step">
                    <div class="success" style="font-size: 18px; text-align: center; padding: 20px 0;">
                        ✓ Setup Complete!
                    </div>
                    <p style="text-align: center;">Your DutyWatch is ready. Redirecting...</p>
                </div>
            </div>
            
            <script>
                const username = '{username}';
                const step1 = document.getElementById('step1');
                const step2 = document.getElementById('step2');
                const step3 = document.getElementById('step3');
                
                document.getElementById('connect-btn').addEventListener('click', async () => {{
                    const user = document.getElementById('icloud-user').value.trim();
                    const pw = document.getElementById('icloud-pw').value.trim();
                    const errEl = document.getElementById('step1-error');
                    
                    if (!user || !pw) {{
                        errEl.textContent = 'Please enter both email and password';
                        errEl.style.display = 'block';
                        return;
                    }}
                    
                    const btn = document.getElementById('connect-btn');
                    btn.disabled = true;
                    btn.textContent = 'Connecting...';
                    errEl.style.display = 'none';
                    
                    try {{
                        const res = await fetch(`/api/users/${{username}}/setup-credentials`, {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{icloud_user: user, icloud_app_pw: pw}})
                        }});
                        const data = await res.json();
                        
                        if (!res.ok) {{
                            throw new Error(data.detail || 'Connection failed');
                        }}
                        
                        // Populate calendar dropdown
                        const select = document.getElementById('calendar-select');
                        select.innerHTML = data.calendars.map(c => 
                            `<option value="${{c}}">${{c}}</option>`
                        ).join('');
                        
                        step1.classList.remove('active');
                        step2.classList.add('active');
                    }} catch (e) {{
                        errEl.textContent = e.message;
                        errEl.style.display = 'block';
                        btn.disabled = false;
                        btn.textContent = 'Connect to iCloud';
                    }}
                }});
                
                document.getElementById('finish-btn').addEventListener('click', async () => {{
                    const cal = document.getElementById('calendar-select').value;
                    const errEl = document.getElementById('step2-error');
                    
                    if (!cal) {{
                        errEl.textContent = 'Please select a calendar';
                        errEl.style.display = 'block';
                        return;
                    }}
                    
                    const btn = document.getElementById('finish-btn');
                    btn.disabled = true;
                    btn.textContent = 'Saving...';
                    errEl.style.display = 'none';
                    
                    try {{
                        const res = await fetch(`/api/users/${{username}}/select-calendar`, {{
                            method: 'POST',
                            headers: {{'Content-Type': 'application/json'}},
                            body: JSON.stringify({{calendar_name: cal}})
                        }});
                        
                        if (!res.ok) {{
                            const data = await res.json();
                            throw new Error(data.detail || 'Failed to save');
                        }}
                        
                        step2.classList.remove('active');
                        step3.classList.add('active');
                        
                        // Redirect to their pairings page
                        setTimeout(() => {{
                            window.location.href = `/${{username}}`;
                        }}, 1500);
                    }} catch (e) {{
                        errEl.textContent = e.message;
                        errEl.style.display = 'block';
                        btn.disabled = false;
                        btn.textContent = 'Finish Setup';
                    }}
                }});
                
                // Allow Enter key to submit
                document.getElementById('icloud-pw').addEventListener('keypress', (e) => {{
                    if (e.key === 'Enter') document.getElementById('connect-btn').click();
                }});
            </script>
        </body>
        </html>
        """
    
    
    # ==========================================
    # Admin Routes
    # ==========================================
    
    @app.get("/api/admin/users")
    async def admin_list_users():
        """List all users (admin only - add auth as needed)."""
        def do_list():
            with get_db_func() as conn:
                return list_all_users(conn)
        
        users = await run_in_threadpool(do_list)
        return {"users": users}


# ==========================================
# Initialization
# ==========================================

def init_multiuser_db(get_db_func):
    """Initialize multi-user tables. Call this from your app startup."""
    with get_db_func() as conn:
        init_multiuser_tables(conn)