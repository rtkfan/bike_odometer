import requests
import os
import json
import datetime
import sqlite3
import humanize
import logging

endpoint_token = 'https://www.strava.com/oauth/token'
endpoint_activities = 'https://www.strava.com/api/v3/athlete/activities'
field_list = """activity_id,athlete_id,gear_id,name,
                start_date,start_date_local,timezone,utc_offset,
                start_lat,start_lng,end_lat,end_lng,
                distance,moving_time,elapsed_time,total_elevation_gain"""
mode = 'INCREMENTAL'  # either INCREMENTAL or FULL, make this an arg later


def check_envvars():
    clientid = os.getenv('STRAVA_CLIENTID')
    clientsecret = os.getenv('STRAVA_CLIENTSECRET')
    athleteid = os.getenv('STRAVA_ATHLETEID')
    if (clientid is None or clientsecret is None or athleteid is None):
        logging.error('Environment variables not set')
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
    logging.info('Latest access token for athlete %s %s %s',
                 last_auth['athlete_id'], 'expire'+ess_or_dee,
                 humanize.naturaltime(-1*last_auth_ttl))

    if last_auth_ttl.total_seconds() <= 3600:  # less than an hour to expiry:
        print('Latest access token has/is close to expiring')

        payload = {'client_id': clientid,
                   'client_secret': clientsecret,
                   'refresh_token': last_auth['refresh_token'],
                   'grant_type': 'refresh_token'}
        r = requests.post(endpoint_token, params=payload)
        logging.info('Request to %s returned status %s',
                     endpoint_token, r.status_code)
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


def map_activity(in_dict):

    if in_dict['start_latlng'] is None:
        startlatlng = [None, None]
    else:
        startlatlng = in_dict['start_latlng']

    if in_dict['end_latlng'] is None:
        endlatlng = [None, None]
    else:
        endlatlng = in_dict['end_latlng']

    out_tuple = (
        in_dict['id'],                    # activity_id
        in_dict['athlete']['id'],         # athlete_id
        in_dict['gear_id'],               # gear_id
        in_dict['name'],                  # name
        in_dict['start_date'],            # start_date
        in_dict['start_date_local'],      # start_date_local
        in_dict['timezone'],              # timezone
        in_dict['utc_offset'],            # utc_offset
        startlatlng[0],                   # start_lat
        startlatlng[1],                   # start_lng
        endlatlng[0],                     # end_lat
        endlatlng[1],                     # end_lng
        in_dict['distance'],              # distance
        in_dict['moving_time'],           # moving_time
        in_dict['elapsed_time'],          # elapsed_time
        in_dict['total_elevation_gain'])  # total_elevation_gain

    return out_tuple


def stage_activities(access_token, connection, latest_activity):

    ipage = 1
    rows_scanned = 0
    rows_loaded = 0

    cur = connection.cursor()
    cur.execute("""DROP TABLE IF EXISTS stg_activity;""")
    cur.execute("""CREATE TABLE stg_activity
                   AS SELECT """ + field_list +
                """ FROM activity WHERE FALSE;""")

    while True:
        payload = {'access_token': access_token,
                   'page': ipage,
                   'after': latest_activity}
        r = requests.get(endpoint_activities, params=payload)
        logging.info('API usage: %s requests last 15 min/today; limit %s',
                     r.headers['X-RateLimit-Usage'],
                     r.headers['X-RateLimit-Limit'])
        response = json.loads(r.text)
        if response == []:
            break
        rows_scanned += len(response)
        rows = [map_activity(i) for i in response if i['type'] == 'Ride']
        cur.executemany("""INSERT INTO stg_activity
                           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        rows)
        rows_loaded += len(rows)
        logging.info('Staged %s rides so far out of %s total activities',
                     rows_loaded, rows_scanned)

        ipage += 1

    connection.commit()

    return rows_scanned, rows_loaded


def insert_staged_new(connection):

    cur = connection.cursor()
    cur.execute("""CREATE TABLE activity_insert AS
                   SELECT s.*,
                          CURRENT_TIMESTAMP,
                          CURRENT_TIMESTAMP
                   FROM stg_activity s
                   LEFT JOIN activity a ON s.activity_id = a.activity_id
                   WHERE a.activity_id IS NULL;""")
    connection.commit()

    cur.execute("""SELECT COUNT(*) AS num FROM activity_insert;""")
    num_insert = cur.fetchone()
    if num_insert['num'] != 0:
        cur.execute("""INSERT INTO activity
                       SELECT * FROM activity_insert""")

    cur.execute("""DROP TABLE IF EXISTS activity_insert;""")
    connection.commit()

    return num_insert['num']


def update_staged_updated(connection):

    cur = connection.cursor()
    cur.execute("""CREATE TABLE activity_update AS
                   SELECT t.*,
                   a.created_at,
                   CURRENT_TIMESTAMP
                   FROM (SELECT """+field_list+""" FROM stg_activity
                   EXCEPT
                   SELECT """+field_list+""" FROM activity) t
                   INNER JOIN activity a ON t.activity_id = a.activity_id""")
    connection.commit()

    cur.execute("""SELECT COUNT(*) AS num FROM activity_update;""")
    num_update = cur.fetchone()
    if num_update['num'] != 0:
        cur.execute("""DELETE FROM activity WHERE activity_id IN
                       (SELECT activity_id FROM activity_update)""")
        cur.execute("""INSERT INTO activity
                       SELECT * FROM activity_update""")

    cur.execute("""DROP TABLE IF EXISTS activity_update;""")
    connection.commit()

    return num_update['num']


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
    logging.info('Fetched access token: %s', access_token)

    if mode == 'FULL':
        after = 0
    else:
        cur = con.cursor()
        cur.execute("""SELECT strftime('%s', MAX(start_date))
                                AS latest_activity
                       FROM activity;""")
        latest_row = cur.fetchone()
        after = latest_row['latest_activity']

    rows_scanned, rows_loaded = stage_activities(access_token, con, after)
    logging.info('%s %s %s %s %s',
                 'Activities staging table loaded;',
                 rows_loaded,
                 'activities loaded of',
                 rows_scanned,
                 'activities total')

    num_inserted = insert_staged_new(con)
    plural_token = 'y' if num_inserted == 1 else 'ies'
    logging.info('Inserted %s new activit%s', num_inserted, plural_token)

    num_updated = update_staged_updated(con)
    plural_token = 'y' if num_inserted == 1 else 'ies'
    logging.info('Updated %s existing activit%s', num_updated, plural_token)

    # close up shop
    cur = con.cursor()
    cur.execute("""DROP TABLE stg_activity""")
    con.commit()
    con.close()


if __name__ == '__main__':
    main()
