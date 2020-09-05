import requests
import os
import json
import datetime
import sqlite3
import humanize
import logging

endpoint_token = 'https://www.strava.com/oauth/token'


def check_envvars():
    clientid = os.getenv('STRAVA_CLIENTID')
    clientsecret = os.getenv('STRAVA_CLIENTSECRET')
    athleteid = os.getenv('STRAVA_ATHLETEID')
    if (clientid is None or clientsecret is None or athleteid is None):
        logging.error('%s %s',
                      'Environment variables (STRAVA_CLIENTID, '
                      'STRAVA_CLIENTSECRET) not set')
        exit(-1)
    return clientid, clientsecret, athleteid


def get_access_token(connection, clientid, clientsecret, athleteid):

    cur = connection.cursor()

    logging.info('Connection established to DB')
    cur.execute("""SELECT * FROM strava_access_token
                   WHERE athlete_id = ?
                   ORDER BY expires_at DESC LIMIT 1""", [str(athleteid)])
    last_auth = cur.fetchone()

    last_auth_expire_utc = datetime.datetime.utcfromtimestamp(
        last_auth['expires_at'])
    last_auth_ttl = last_auth_expire_utc - datetime.datetime.utcnow()
    # last_auth_ttl is positive if expiry is in the future;
    # humanize assumes the opposite!
    ess_or_dee = 'd' if last_auth_ttl.total_seconds() < 0 else 's'
    logging.info('%s %s %s %s',
                 'Latest access token for athlete',
                 last_auth['athlete_id'], 'expire'+ess_or_dee,
                 humanize.naturaltime(-1*last_auth_ttl))

    if last_auth_ttl.total_seconds() <= 3600:  # less than an hour to expiry:
        print('Latest access token has/is close to expiring')

        payload = {'client_id': clientid,
                   'client_secret': clientsecret,
                   'refresh_token': last_auth['refresh_token'],
                   'grant_type': 'refresh_token'}
        r = requests.post(endpoint_token, params=payload)
        logging.info('%s %s %s %s',
                     'Request to', endpoint_token,
                     'returned status code', r.status_code)
        response = json.loads(r.text)

        cur.execute("""INSERT INTO strava_access_token(athlete_id,
                       access_token, expires_at, refresh_token)
                       VALUES (?,?,?,?)""",
                    [last_auth['athlete_id'], response['access_token'],
                     response['expires_at'], response['refresh_token']])
        connection.commit()
        valid_access_token = response['access_token']
    else:
        valid_access_token = last_auth['access_token']

    return valid_access_token


def main():

    logging.basicConfig(format='%(levelname)s %(asctime)s: %(message)s',
                        level=logging.INFO,
                        datefmt='%H:%M:%S')

    strava_clientid, strava_clientsecret, athlete_id = check_envvars()

    # connect to db
    con = sqlite3.connect('./data/odometer.db')
    con.row_factory = sqlite3.Row

    access_token = get_access_token(con, strava_clientid, strava_clientsecret,
                                    athlete_id)
    logging.info('%s %s', 'Fetched access token ', access_token)

    # close up shop
    con.close()


if __name__ == '__main__':
    main()
