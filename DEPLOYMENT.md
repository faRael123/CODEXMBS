# Render Deployment Notes

## Recommended Path

Use the existing Render web service and PostgreSQL database:

- Web service: `CODEXMBS`
- Database: `codexmbs_db`

Do not create a new service unless you want a separate staging copy. Updating the current service keeps the same public URL and database wiring.

## Required Environment Variables

Set these on the Render web service:

```text
SECRET_KEY=<long random generated value>
FLASK_ENV=production
APP_ENV=production
DATABASE_URL=<Render PostgreSQL internal database URL>
GMAIL_USER=gajoda.system@gmail.com
GMAIL_APP_PASSWORD=<16-character Gmail app password>
APP_BASE_URL=<your Render public web service URL>
```

Optional:

```text
FLASK_DEBUG=0
```

Do not set the old MySQL variables on Render. The app now uses `DATABASE_URL` for PostgreSQL.

## Render Build And Start Commands

Build command:

```sh
pip install -r requirements.txt
```

Pre-deploy command:

```sh
python -c "from app import initialize_database; initialize_database()"
```

Start command:

```sh
gunicorn app:app --worker-class gthread --threads 100 --bind 0.0.0.0:$PORT
```

The included `Procfile` already contains the same start command.

## Database

The app now uses PostgreSQL through `psycopg2-binary` and Render's `DATABASE_URL`.

Run the pre-deploy command after setting `DATABASE_URL`; it creates the tables and seeds starter data. The seed process is idempotent, so it can run during deploys.

## Starter Accounts

Change these before treating the deployment as production:

```text
superadmin / superadmin123
admin / admin123
driver1 / driver123
conductor1 / conductor123
```

After the first successful deploy, sign in as super admin and replace demo accounts or passwords.

## Password Reset Email

`APP_BASE_URL` must match the deployed Render URL, for example:

```text
APP_BASE_URL=https://codexmbs.onrender.com
```

If `APP_BASE_URL` is wrong, password reset emails will point to the wrong host.
