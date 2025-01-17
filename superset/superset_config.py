import os

# Set the SQLAlchemy database URI to connect Superset to MySQL database for metadata storage
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SQLALCHEMY_DATABASE_URI", "mysql://root:rootpassword@mysql:3306/superset_metadata"
)

# Set up the Flask app's secret key for signing cookies, CSRF tokens, etc.
APP_NAME = "Superset"

# Default role for new users (like 'Admin' or 'Gamma')
DEFAULT_ROLE = "Gamma"

# Set the allowed time range for SQL Lab queries (in seconds)
SQLLAB_DEFAULT_MAX_RUNNING_QUERY = 300  # 5 minutes

# Enable SQL Lab (query editor)
SQLLAB_CTAS_CREATE = True  # Allow create table as (CTAS) queries

# Set the maximum file upload size in bytes (default is 100 MB)
MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB

# Configuring the security of public dashboards
PUBLIC_ROLE_LIKE = "Gamma"

# Enable or disable SSL support for Superset
ENABLE_PROXY_FIX = True
