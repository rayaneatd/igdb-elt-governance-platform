import os
import secrets
from functools import wraps
from typing import Any
from flask import jsonify, session
from werkzeug.security import check_password_hash


def _verify_credential(provided_password: str, expected_secret: str) -> bool:
    """
    Verifies a provided password against an expected secret.
    Supports either pre-hashed secrets (e.g. scrypt, pbkdf2) or plaintext passwords
    with constant-time comparison to prevent timing attacks.
    """
    if not provided_password or not expected_secret:
        return False

    if expected_secret.startswith(("scrypt:", "pbkdf2:", "argon2:")):
        return check_password_hash(expected_secret, provided_password)

    return secrets.compare_digest(provided_password, expected_secret)


def authenticate_user(username: str, password: str) -> dict[str, Any] | None:
    """
    Validates user credentials against environment variables.
    Returns user payload with role on success, or None on failure.
    """
    if not username or not password:
        return None

    username_clean = username.strip()

    admin_username = os.getenv("ADMIN_USERNAME", "admin").strip()
    admin_password = os.getenv("ADMIN_PASSWORD", "admin123")

    viewer_username = os.getenv("VIEWER_USERNAME", "visitor").strip()
    viewer_password = os.getenv("VIEWER_PASSWORD", "visitor123")

    # Check Admin credentials
    if secrets.compare_digest(username_clean.lower(), admin_username.lower()):
        if _verify_credential(password, admin_password):
            return {"username": admin_username, "role": "ADMIN"}

    # Check Viewer credentials
    if secrets.compare_digest(username_clean.lower(), viewer_username.lower()):
        if _verify_credential(password, viewer_password):
            return {"username": viewer_username, "role": "VIEWER"}

    return None


def login_required(f):
    """Decorator ensuring that the client has an active authenticated session."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("user"):
            return jsonify({"error": "Unauthorized: Please log in."}), 401
        return f(*args, **kwargs)
    return decorated_function


def admin_required(f):
    """Decorator ensuring that the authenticated user possesses the ADMIN role."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user = session.get("user")
        if not user:
            return jsonify({"error": "Unauthorized: Please log in."}), 401
        if user.get("role") != "ADMIN":
            return jsonify({
                "error": "Access denied: This operation requires ADMIN privileges."
            }), 403
        return f(*args, **kwargs)
    return decorated_function

