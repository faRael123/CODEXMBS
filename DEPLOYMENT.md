# Deployment Notes

## Render Environment

Create a Render PostgreSQL database, then copy its internal database URL into the
web service environment.

Set these values on the Render web service:

```text
DATABASE_URL=<Render PostgreSQL internal database URL>
SECRET_KEY=<long random value>
FLASK_ENV=production
```

You can also use individual PostgreSQL variables instead of `DATABASE_URL`:

```text
DB_HOST=<postgres host>
DB_PORT=5432
DB_USER=<postgres user>
DB_PASSWORD=<postgres password>
DB_NAME=<postgres database name>
```

`DATABASE_URL` is preferred on Render because it is copied directly from the
PostgreSQL service and avoids mismatched host, username, or database values.

## Database Setup

Run schema setup and starter data seeding once after the PostgreSQL variables are
configured:

```sh
python -c "from app import initialize_database; initialize_database()"
```

The temporary starter admin account remains:

```text
username: admin
password: admin123
```

Change this password immediately after the first successful login.

## Render Web Service

Use this build command:

```sh
pip install -r requirements.txt
```

Use this start command:

```sh
gunicorn app:app --worker-class gthread --threads 100 --bind 0.0.0.0:10000
```

For local Windows hosting, use Waitress:

```sh
waitress-serve --listen=0.0.0.0:8000 app:app
```
