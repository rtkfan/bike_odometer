# Bike Odometer
Uses the Strava API to populate a SQLite database with activities information.

## Environment Variables
Requires the following environment variables to be set:

- STRAVA_CLIENTID
- STRAVA_CLIENTSECRET
- STRAVA_ATHLETEID

The script authenticates with Strava using the environment variables listed above, and stores the relevant tokens in the SQLite database as well.
