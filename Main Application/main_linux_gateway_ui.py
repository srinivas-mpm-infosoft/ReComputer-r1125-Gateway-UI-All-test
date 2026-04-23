# #!/usr/bin/env python3

# import os
# import json
# import bcrypt
# import logging
# from datetime import datetime, timedelta
# from pathlib import Path
# import subprocess
# import time

# from flask import (
#     Flask, request, jsonify, send_from_directory, session
# )
# from flask_sqlalchemy import SQLAlchemy

# from flask_cors import CORS



# # ============================================================
# # PATHS
# # ============================================================

# BASE = Path("/home/pi/Downloads/Gateway-UI/Main Application")
# STATIC = BASE / "static"

# CONFIG_FILE = BASE / "config.json"
# IS_FILE_TO_DB_UPDATED = BASE / "is_file_to_db_updated.json"
# UPDATES_FILE = BASE / "updated.json"
# IS_UPDATED_FILE = BASE / "is_updated.json"

# DEFAULT_FILES = {
#     CONFIG_FILE: {},
#     UPDATES_FILE: [],
#     IS_FILE_TO_DB_UPDATED: False,
#     IS_UPDATED_FILE: False
# }

# for f, d in DEFAULT_FILES.items():
#     if not f.exists():
#         f.write_text(json.dumps(d, indent=2))


# # ============================================================
# # FLASK APP
# # ============================================================

# app = Flask(
#     __name__,
#     static_folder=str(STATIC),
#     static_url_path="/static"
# )

# CORS(
#     app,
#     supports_credentials=True,
#     origins=[
#         "http://localhost:5173",   # React dev
#         "http://127.0.0.1:5173",
#     ]
# )

# # NEVER hardcode this in real life
# app.secret_key = "X7f1m+oJ6q8wR2t9UeY3pF4zN0hKd1sQjM5aV8bZc2xT7nL0oR5vH3gC6dP9yW4k"

# app.config.update(
#     SESSION_COOKIE_HTTPONLY=True,
#     SESSION_COOKIE_SECURE=False,  # TRUE in HTTPS
#     SESSION_COOKIE_SAMESITE="Lax",
#     PERMANENT_SESSION_LIFETIME=timedelta(days=365)
# )

# # ============================================================
# # DATABASE
# # ============================================================

# app.config["SQLALCHEMY_DATABASE_URI"] = (
#     "mysql+pymysql://gateway:gateway@localhost/users"
# )
# app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
# app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
#     "pool_pre_ping": True,
#     "pool_recycle": 1800
# }

# db = SQLAlchemy(app)
# # ============================================================
# # LOGGING
# # ============================================================

# # ============================================================
# # LOGGING (DATE-AWARE)
# # ============================================================

# LOG_ROOT = Path("/home/pi/logs")
# LOGGER_NAME = "configuration_ui"

# _current_log_date = None

# def setup_logging():
#     global _current_log_date

#     today = datetime.now().strftime("%Y-%m-%d")
#     logger = logging.getLogger(LOGGER_NAME)

#     if _current_log_date == today and logger.handlers:
#         return logger   # ✅ already correct for today

#     _current_log_date = today

#     log_dir = LOG_ROOT / today / "configuration_ui"
#     log_dir.mkdir(parents=True, exist_ok=True)

#     log_file = log_dir / "ui.log"

#     formatter = logging.Formatter(
#         "%(asctime)s [%(levelname)s] %(message)s"
#     )

#     logger.setLevel(logging.INFO)
#     logger.handlers.clear()
#     logger.propagate = False

#     fh = logging.FileHandler(log_file)
#     fh.setFormatter(formatter)

#     sh = logging.StreamHandler()
#     sh.setFormatter(formatter)

#     logger.addHandler(fh)
#     logger.addHandler(sh)

#     logger.info("📁 Logging initialized → %s", log_file)
#     return logger


# @app.before_request
# def refresh_logging():
#     setup_logging()

# # ============================================================
# # MODEL (SINGLE TABLE)
# # ============================================================

# class User(db.Model):
#     __tablename__ = "user_details"

#     id = db.Column(db.Integer, primary_key=True)
#     username = db.Column(db.String(255), unique=True, nullable=False)
#     password_hash = db.Column(db.Text, nullable=False)
#     role = db.Column(db.Enum("superadmin", "admin", "user"), nullable=False)

# # ============================================================
# # REQUEST / RESPONSE LOGGING
# # ============================================================

# @app.before_request
# def log_request():
#     log.info(
#         "REQ method=%s path=%s user=%s ip=%s",
#         request.method,
#         request.path,
#         session.get("user"),
#         request.remote_addr
#     )

# @app.after_request
# def log_response(response):
#     log.info(
#         "RESP status=%s path=%s user=%s",
#         response.status_code,
#         request.path,
#         session.get("user")
#     )
#     return response

# # ============================================================
# # MySQL Check
# # ============================================================

# import socket

# def is_mysql_up(host="127.0.0.1",port=3306,timeout=2):
#     try:
#         with socket.create_connection((host, port), timeout = timeout):
#             return True
#     except Exception:
#         return False


# # ============================================================
# # AUTH HELPERS
# # ============================================================

# def require_login():
#     if "user" not in session:
#         log.warning(
#             "UNAUTHORIZED path=%s ip=%s",
#             request.path,
#             request.remote_addr
#         )
#         return jsonify(error="Unauthorized"), 401

# def current_user():
#     return session.get("user")

# def require_role(*roles):
#     if "user" not in session:
#         return jsonify(error="Unauthorized"), 401

#     if session["user"]["role"] not in roles:
#         log.warning(
#             "FORBIDDEN user=%s required=%s",
#             session.get("user"), roles
#         )
#         return jsonify(error="Forbidden"), 403

# # ============================================================
# # UI ROUTES
# # ============================================================

# @app.route("/")
# def home():
#     if "user" not in session:
#         return send_from_directory(STATIC, "login.html")
#     return send_from_directory(STATIC, "index.html")

# # ============================================================
# # AUTH ROUTES
# # ============================================================
# def can_create_user(creator_role: str, target_role: str) -> bool:
#     if creator_role == "superadmin":
#         return target_role in ("superadmin", "admin", "user")

#     if creator_role == "admin":
#         return target_role == "user"

#     return False

# def create_user_internal(
#     *,
#     username: str,
#     password: str,
#     role: str,
#     creator_username: str,
#     creator_role: str,
# ):
#     username = (username or "").strip()

#     if not username or not password:
#         raise ValueError("Username and password required")

#     if role not in ("superadmin", "admin", "user"):
#         raise ValueError("Invalid role")

#     if not can_create_user(creator_role, role):
#         raise PermissionError(
#             f"{creator_role} cannot create user with role {role}"
#         )

#     if User.query.filter_by(username=username).first():
#         raise RuntimeError("User already exists")

#     password_hash = bcrypt.hashpw(
#         password.encode(),
#         bcrypt.gensalt()
#     ).decode()

#     user = User(
#         username=username,
#         password_hash=password_hash,
#         role=role,
#     )

#     db.session.add(user)
#     db.session.commit()

#     log.info(
#         "USER_CREATED by=%s (%s) username=%s role=%s",
#         creator_username,
#         creator_role,
#         username,
#         role,
#     )

#     return user


# @app.route("/create-user", methods=["POST"])
# def create_user():
#     # Only superadmin can create users
#     # auth = require_role("superadmin")
#     # if auth:
#     #     return auth

#     data = request.json or {}
#     username = data.get("username", "").strip()
#     password = data.get("password", "")
#     role = data.get("role", "user")

#     if not username or not password:
#         return jsonify(error="Username and password required"), 400

#     if role not in ("superadmin", "admin", "user"):
#         return jsonify(error="Invalid role"), 400

#     if User.query.filter_by(username=username).first():
#         log.warning("CREATE_USER_FAIL_EXISTS username=%s", username)
#         return jsonify(error="User already exists"), 409

#     password_hash = bcrypt.hashpw(
#         password.encode(),
#         bcrypt.gensalt()
#     ).decode()

#     user = User(
#         username=username,
#         password_hash=password_hash,
#         role=role
#     )

#     db.session.add(user)
#     db.session.commit()

#     log.info(
#         "USER_CREATED by=%s username=%s role=%s",
#         session.get("user"), username, role
#     )

#     return jsonify(status="user_created")


# @app.route("/login", methods=["POST"])
# def login():
#     data = request.json or {}
#     username = data.get("username", "").strip()
#     password = data.get("password", "")

#     log.info("LOGIN_ATTEMPT user=%s", username)

#     user = User.query.filter_by(username=username).first()
#     if not user:
#         log.warning("LOGIN_FAIL_NOUSER user=%s", username)
#         return jsonify(error="Invalid credentials"), 401

#     if not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
#         log.warning("LOGIN_FAIL_BADPASS user=%s", username)
#         return jsonify(error="Invalid credentials"), 401

#     session.permanent = True
#     session["user"] = {
#         "username": user.username,
#         "role": user.role
#     }

#     log.info("LOGIN_SUCCESS user=%s role=%s", user.username, user.role)
#     return jsonify(status="success")

# @app.route("/reset-password", methods=["POST"])
# def reset_password():
#     # Must be logged in
#     if "user" not in session:
#         log.warning("RESET_PASSWORD_UNAUTHORIZED ip=%s", request.remote_addr)
#         return jsonify(error="Unauthorized"), 401

#     data = request.json or {}

#     old_password = data.get("oldPassword", "")
#     new_password = data.get("newPassword", "")
#     login_req = data.get("login_req", False)

#     if not old_password or not new_password:
#         return jsonify(error="Old and new password required"), 400

#     username = session["user"]["username"]

#     user = User.query.filter_by(username=username).first()
#     if not user:
#         log.error("RESET_PASSWORD_NOUSER session_user=%s", session.get("user"))
#         return jsonify(error="User not found"), 404

#     # Verify old password
#     if not bcrypt.checkpw(
#         old_password.encode(),
#         user.password_hash.encode()
#     ):
#         log.warning("RESET_PASSWORD_BAD_OLD user=%s", username)
#         return jsonify(error="Old password is incorrect"), 401

#     # OPTIONAL: basic sanity check
#     if len(new_password) < 6:
#         return jsonify(error="Password too short"), 400

#     # Hash new password
#     new_hash = bcrypt.hashpw(
#         new_password.encode(),
#         bcrypt.gensalt()
#     ).decode()

#     user.password_hash = new_hash
#     db.session.commit()

#     log.info("PASSWORD_CHANGED user=%s", username)

#     # Optional: force re-login if requested
#     if login_req:
#         session.clear()

#     return jsonify(status="password_updated")

# @app.route("/logout", methods=["POST"])
# def logout():
#     log.info("LOGOUT user=%s", session.get("user"))
#     session.clear()
#     return jsonify(status="logged_out")

# @app.route("/whoami")
# def whoami():
#     if "user" not in session:
#         return jsonify(error="Unauthorized"), 401
#     return jsonify(session["user"])


# # ============================================================
# # GPIO Status
# # ============================================================


# GPIO_PINS = {
#     "Digital Output 1": 24,
#     "Digital Output 2": 25,
#     "Digital Output 3": 26,
#     "Digital Output 4": 6
# }

# def get_gpio_level(pin):
#     try:
#         result = subprocess.run(
#             ["raspi-gpio", "get", str(pin)],
#             capture_output=True,
#             text=True
#         )

#         output = result.stdout.strip()

#         # Example output:
#         # GPIO 24: level=1 fsel=1 func=OUTPUT pull=NONE

#         if "level=1" in output:
#             return "HIGH"
#         elif "level=0" in output:
#             return "LOW"
#         else:
#             return "UNKNOWN"

#     except Exception as e:
#         return f"ERROR: {str(e)}"


# @app.route("/gpio-status", methods=["GET"])
# def get_gpio_status():
#     if require_login():
#         return require_login()

#     status = {}

#     for name, pin in GPIO_PINS.items():
#         status[name] = get_gpio_level(pin)

#     return jsonify(status)

# # ============================================================
# # CONFIG ROUTES
# # ============================================================

# @app.route("/config", methods=["GET"])
# def get_config():
#     if require_login():
#         return require_login()
#     return jsonify(json.loads(CONFIG_FILE.read_text()))


# @app.route("/config", methods=["POST"])
# def set_config():
#     if require_login():
#         return require_login()

#     data = request.json or {}
#     print(f"Config update received: {data}")

#     # Load existing config
#     current = json.loads(CONFIG_FILE.read_text())

#     changed_fields = {}
#     updated = False

#     for key, new_value in data.items():
#         old_value = current.get(key)

#         # Update only if value actually changed
#         if old_value != new_value:
#             current[key] = new_value
#             changed_fields[key] = {
#                 "old": old_value,
#                 "new": new_value
#             }
#             updated = True

#     # Nothing changed → don't touch disk
#     if not updated:
#         return jsonify({
#             "status": "ok",
#             "message": "No changes detected"
#         })

#     # Persist changes
#     CONFIG_FILE.write_text(json.dumps(current, indent=2))
#     IS_FILE_TO_DB_UPDATED.write_text(json.dumps(True, indent=2))

#     # Log only what changed
#     log_update(fields=list(changed_fields.keys()))

#     return jsonify({
#         "status": "ok",
#         "changed": changed_fields
#     })


# # ============================================================
# # UPDATE TRACKING
# # ============================================================

# def log_update(fields):
#     updates = json.loads(UPDATES_FILE.read_text())
#     updates.append({
#         "ts": datetime.now().isoformat(),
#         "user": current_user(),
#         "fields": fields
#     })
#     UPDATES_FILE.write_text(json.dumps(updates, indent=2))
    
#     IS_UPDATED_FILE.write_text("true")

#     log.info("CONFIG_UPDATED user=%s fields=%s", current_user(), fields)

# @app.route("/update-status")
# def update_status():
#     updates = json.loads(UPDATES_FILE.read_text())
#     return jsonify(updates[-1] if updates else {})

# @app.route("/clear-update-flag", methods=["POST"])
# def clear_update():
#     IS_UPDATED_FILE.write_text("false")
#     log.info("UPDATE_FLAG_CLEARED user=%s", current_user())
#     return jsonify(status="cleared")

# # ============================================================
# # MAIN
# # ============================================================
# if __name__ == "__main__":
#     log = setup_logging()
#     log.info("Starting Innodose Configuration UI")
#             # ✅ MUST BE INSIDE app.app_context
#     try:
#         create_user_internal(
#             username="superadmin",
#             password="superadmin",
#             role="superadmin",
#             creator_username="bootstrap",
#             creator_role="superadmin",
#         )
#         log.info("Bootstrap superadmin ensured")
#     except RuntimeError:
#         log.info("Bootstrap superadmin already exists")
#     with app.app_context():
#         for retry in range(10):
#             if is_mysql_up():
#                 try:
#                     db.create_all()
#                     log.info("Database ready")
#                     break
#                 except Exception as e:
#                     log.error("DB init failed: %s", e)
#             else:
#                 log.warning("MySQL is down at startup — UI running without DB")
#             time.sleep(2)



#     # ✅ app context ends here
#     app.run(host="0.0.0.0", port=8000, threaded=True)



#!/usr/bin/env python3

import os
import json
import bcrypt
import logging
from datetime import datetime, timedelta
from pathlib import Path
import subprocess
import time

from flask import (
    Flask, request, jsonify, send_from_directory, session
)
from flask_sqlalchemy import SQLAlchemy

from flask_cors import CORS



# ============================================================
# PATHS
# ============================================================

BASE = Path("/home/pi/Downloads/Gateway-UI/Main Application")
STATIC = BASE / "static"

CONFIG_FILE = BASE / "config.json"
IS_FILE_TO_DB_UPDATED = BASE / "is_file_to_db_updated.json"
UPDATES_FILE = BASE / "updated.json"
IS_UPDATED_FILE = BASE / "is_updated.json"

DEFAULT_FILES = {
    CONFIG_FILE: {},
    UPDATES_FILE: [],
    IS_FILE_TO_DB_UPDATED: False,
    IS_UPDATED_FILE: False
}

for f, d in DEFAULT_FILES.items():
    if not f.exists():
        f.write_text(json.dumps(d, indent=2))


# ============================================================
# FLASK APP
# ============================================================

app = Flask(
    __name__,
    static_folder=str(STATIC),
    static_url_path="/static"
)

CORS(
    app,
    supports_credentials=True,
    origins=[
        "http://localhost:5173",   # React dev
        "http://127.0.0.1:5173",
    ]
)

# NEVER hardcode this in real life
app.secret_key = "X7f1m+oJ6q8wR2t9UeY3pF4zN0hKd1sQjM5aV8bZc2xT7nL0oR5vH3gC6dP9yW4k"

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=False,  # TRUE in HTTPS
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=365)
)

# ============================================================
# DATABASE
# ============================================================

app.config["SQLALCHEMY_DATABASE_URI"] = (
    "mysql+pymysql://gateway:gateway@localhost/users"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_pre_ping": True,
    "pool_recycle": 1800
}

db = SQLAlchemy(app)
# ============================================================
# LOGGING
# ============================================================

# ============================================================
# LOGGING (DATE-AWARE)
# ============================================================

LOG_ROOT = Path.home() / "logs"
LOGGER_NAME = "configuration_ui"

_current_log_date = None

def setup_logging():
    global _current_log_date

    today = datetime.now().strftime("%Y-%m-%d")
    logger = logging.getLogger(LOGGER_NAME)

    if _current_log_date == today and logger.handlers:
        return logger   # ✅ already correct for today

    _current_log_date = today

    log_dir = LOG_ROOT / today / "configuration_ui"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / "ui.log"

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    logger.setLevel(logging.INFO)
    
    if logger.handlers:
        return logger
    
    logger.propagate = False

    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    logger.info("📁 Logging initialized → %s", log_file)
    return logger


@app.before_request
def refresh_logging():
    setup_logging()

# ============================================================
# MODEL (SINGLE TABLE)
# ============================================================

class User(db.Model):
    __tablename__ = "user_details"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(255), unique=True, nullable=False)
    password_hash = db.Column(db.Text, nullable=False)
    role = db.Column(db.Enum("superadmin", "admin", "user"), nullable=False)

# ============================================================
# REQUEST / RESPONSE LOGGING
# ============================================================

@app.before_request
def log_request():
    log.info(
        "REQ method=%s path=%s user=%s ip=%s",
        request.method,
        request.path,
        session.get("user"),
        request.remote_addr
    )

@app.after_request
def log_response(response):
    log.info(
        "RESP status=%s path=%s user=%s",
        response.status_code,
        request.path,
        session.get("user")
    )
    return response

# ============================================================
# MySQL Check
# ============================================================

import socket

def is_mysql_up(host="127.0.0.1",port=3306,timeout=2):
    try:
        with socket.create_connection((host, port), timeout = timeout):
            return True
    except Exception:
        return False


# ============================================================
# AUTH HELPERS
# ============================================================

def require_login():
    if "user" not in session:
        log.warning(
            "UNAUTHORIZED path=%s ip=%s",
            request.path,
            request.remote_addr
        )
        return jsonify(error="Unauthorized"), 401

def current_user():
    return session.get("user")

def require_role(*roles):
    if "user" not in session:
        return jsonify(error="Unauthorized"), 401

    if session["user"]["role"] not in roles:
        log.warning(
            "FORBIDDEN user=%s required=%s",
            session.get("user"), roles
        )
        return jsonify(error="Forbidden"), 403

# ============================================================
# UI ROUTES
# ============================================================

@app.route("/")
def home():
    if "user" not in session:
        return send_from_directory(STATIC, "login.html")
    return send_from_directory(STATIC, "index.html")

# ============================================================
# AUTH ROUTES
# ============================================================
def can_create_user(creator_role: str, target_role: str) -> bool:
    if creator_role == "superadmin":
        return target_role in ("superadmin", "admin", "user")

    if creator_role == "admin":
        return target_role == "user"

    return False

def create_user_internal(
    *,
    username: str,
    password: str,
    role: str,
    creator_username: str,
    creator_role: str,
):
    username = (username or "").strip()

    if not username or not password:
        raise ValueError("Username and password required")

    if role not in ("superadmin", "admin", "user"):
        raise ValueError("Invalid role")

    if not can_create_user(creator_role, role):
        raise PermissionError(
            f"{creator_role} cannot create user with role {role}"
        )

    if User.query.filter_by(username=username).first():
        raise RuntimeError("User already exists")

    password_hash = bcrypt.hashpw(
        password.encode(),
        bcrypt.gensalt()
    ).decode()

    user = User(
        username=username,
        password_hash=password_hash,
        role=role,
    )

    db.session.add(user)
    db.session.commit()

    log.info(
        "USER_CREATED by=%s (%s) username=%s role=%s",
        creator_username,
        creator_role,
        username,
        role,
    )

    return user


@app.route("/create-user", methods=["POST"])
def create_user():
    # Only superadmin can create users
    # auth = require_role("superadmin")
    # if auth:
    #     return auth

    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    role = data.get("role", "user")

    if not username or not password:
        return jsonify(error="Username and password required"), 400

    if role not in ("superadmin", "admin", "user"):
        return jsonify(error="Invalid role"), 400

    if User.query.filter_by(username=username).first():
        log.warning("CREATE_USER_FAIL_EXISTS username=%s", username)
        return jsonify(error="User already exists"), 409

    password_hash = bcrypt.hashpw(
        password.encode(),
        bcrypt.gensalt()
    ).decode()

    user = User(
        username=username,
        password_hash=password_hash,
        role=role
    )

    db.session.add(user)
    db.session.commit()

    log.info(
        "USER_CREATED by=%s username=%s role=%s",
        session.get("user"), username, role
    )

    return jsonify(status="user_created")


@app.route("/login", methods=["POST"])
def login():
    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")

    log.info("LOGIN_ATTEMPT user=%s", username)

    user = User.query.filter_by(username=username).first()
    if not user:
        log.warning("LOGIN_FAIL_NOUSER user=%s", username)
        return jsonify(error="Invalid credentials"), 401

    if not bcrypt.checkpw(password.encode(), user.password_hash.encode()):
        log.warning("LOGIN_FAIL_BADPASS user=%s", username)
        return jsonify(error="Invalid credentials"), 401

    session.permanent = True
    session["user"] = {
        "username": user.username,
        "role": user.role
    }

    log.info("LOGIN_SUCCESS user=%s role=%s", user.username, user.role)
    return jsonify(status="success")

@app.route("/reset-password", methods=["POST"])
def reset_password():
    # Must be logged in
    if "user" not in session:
        log.warning("RESET_PASSWORD_UNAUTHORIZED ip=%s", request.remote_addr)
        return jsonify(error="Unauthorized"), 401

    data = request.json or {}

    old_password = data.get("oldPassword", "")
    new_password = data.get("newPassword", "")
    login_req = data.get("login_req", False)

    if not old_password or not new_password:
        return jsonify(error="Old and new password required"), 400

    username = session["user"]["username"]

    user = User.query.filter_by(username=username).first()
    if not user:
        log.error("RESET_PASSWORD_NOUSER session_user=%s", session.get("user"))
        return jsonify(error="User not found"), 404

    # Verify old password
    if not bcrypt.checkpw(
        old_password.encode(),
        user.password_hash.encode()
    ):
        log.warning("RESET_PASSWORD_BAD_OLD user=%s", username)
        return jsonify(error="Old password is incorrect"), 401

    # OPTIONAL: basic sanity check
    if len(new_password) < 6:
        return jsonify(error="Password too short"), 400

    # Hash new password
    new_hash = bcrypt.hashpw(
        new_password.encode(),
        bcrypt.gensalt()
    ).decode()

    user.password_hash = new_hash
    db.session.commit()

    log.info("PASSWORD_CHANGED user=%s", username)

    # Optional: force re-login if requested
    if login_req:
        session.clear()

    return jsonify(status="password_updated")

@app.route("/logout", methods=["POST"])
def logout():
    log.info("LOGOUT user=%s", session.get("user"))
    session.clear()
    return jsonify(status="logged_out")

@app.route("/whoami")
def whoami():
    if "user" not in session:
        return jsonify(error="Unauthorized"), 401
    return jsonify(session["user"])


# ============================================================
# GPIO Status
# ============================================================


GPIO_PINS = {
    "Digital Output 1": 24,
    "Digital Output 2": 25,
    "Digital Output 3": 26,
    "Digital Output 4": 6
}

def get_gpio_level(pin):
    try:
        result = subprocess.run(
            ["raspi-gpio", "get", str(pin)],
            capture_output=True,
            text=True
        )

        output = result.stdout.strip()

        # Example output:
        # GPIO 24: level=1 fsel=1 func=OUTPUT pull=NONE

        if "level=1" in output:
            return "HIGH"
        elif "level=0" in output:
            return "LOW"
        else:
            return "UNKNOWN"

    except Exception as e:
        return f"ERROR: {str(e)}"


@app.route("/gpio-status", methods=["GET"])
def get_gpio_status():
    if require_login():
        return require_login()

    status = {}

    for name, pin in GPIO_PINS.items():
        status[name] = get_gpio_level(pin)

    return jsonify(status)

# ============================================================
# CONFIG ROUTES
# ============================================================

@app.route("/config", methods=["GET"])
def get_config():
    if require_login():
        return require_login()
    return jsonify(json.loads(CONFIG_FILE.read_text()))


@app.route("/config", methods=["POST"])
def set_config():
    if require_login():
        return require_login()

    data = request.json or {}
    print(f"Config update received: {data}")

    # Load existing config
    current = json.loads(CONFIG_FILE.read_text())

    changed_fields = {}
    updated = False

    for key, new_value in data.items():
        old_value = current.get(key)

        # Update only if value actually changed
        if old_value != new_value:
            current[key] = new_value
            changed_fields[key] = {
                "old": old_value,
                "new": new_value
            }
            updated = True

    # Nothing changed → don't touch disk
    if not updated:
        return jsonify({
            "status": "ok",
            "message": "No changes detected"
        })

    # Persist changes
    CONFIG_FILE.write_text(json.dumps(current, indent=2))
    IS_FILE_TO_DB_UPDATED.write_text(json.dumps(True, indent=2))

    # Log only what changed
    log_update(fields=list(changed_fields.keys()))

    return jsonify({
        "status": "ok",
        "changed": changed_fields
    })


# ============================================================
# UPDATE TRACKING
# ============================================================

def log_update(fields):
    updates = json.loads(UPDATES_FILE.read_text())
    updates.append({
        "ts": datetime.now().isoformat(),
        "user": current_user(),
        "fields": fields
    })
    UPDATES_FILE.write_text(json.dumps(updates, indent=2))
    
    IS_UPDATED_FILE.write_text("true")

    log.info("CONFIG_UPDATED user=%s fields=%s", current_user(), fields)

@app.route("/update-status")
def update_status():
    updates = json.loads(UPDATES_FILE.read_text())
    return jsonify(updates[-1] if updates else {})

@app.route("/clear-update-flag", methods=["POST"])
def clear_update():
    IS_UPDATED_FILE.write_text("false")
    log.info("UPDATE_FLAG_CLEARED user=%s", current_user())
    return jsonify(status="cleared")

# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    log = setup_logging()
    log.info("Starting Innodose Configuration UI")
            # ✅ MUST BE INSIDE app.app_context
    try:
        create_user_internal(
            username="superadmin",
            password="superadmin",
            role="superadmin",
            creator_username="bootstrap",
            creator_role="superadmin",
        )
        log.info("Bootstrap superadmin ensured")
    except RuntimeError:
        log.info("Bootstrap superadmin already exists")
    with app.app_context():
        for retry in range(10):
            if is_mysql_up():
                try:
                    db.create_all()
                    log.info("Database ready")
                    break
                except Exception as e:
                    log.error("DB init failed: %s", e)
            else:
                log.warning("MySQL is down at startup — UI running without DB")
            time.sleep(2)



    # ✅ app context ends here
    app.run(host="0.0.0.0", port=8000, threaded=True)
