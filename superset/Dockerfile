# Use the official Apache Superset image as the base image
FROM apache/superset:latest

# Install system dependencies for mysqlclient
RUN apt-get update && apt-get install -y \
    python3-dev \
    libmysqlclient-dev \
    build-essential \
    && pip install --no-cache-dir mysqlclient

# Set environment variables for the Superset database connection
ENV SUPERSET_SECRET_KEY=mysecretkey
ENV SQLALCHEMY_DATABASE_URI=mysql://superset:superset_password@db:3306/superset_metadata
ENV REDIS_URL=redis://redis:6379/0

# Expose Superset's default port
EXPOSE 8088

# Copy the custom Superset config file (optional)
COPY ./superset_config.py /app/pythonpath/superset_config.py

# Initialize the database and create the admin user during container startup
CMD ["bash", "-c", "
    # Wait for the MySQL database to be ready
    until nc -z db 3306; do
      echo 'Waiting for MySQL...';
      sleep 5;
    done;

    # Run database migrations
    superset db upgrade;

    # Create the Superset admin user
    superset fab create-admin --username admin --firstname Simon --lastname Jaramillo --email admin@admin.com --password admin;

    # Start the Superset server
    superset run -h 0.0.0.0 -p 8088;
"]
