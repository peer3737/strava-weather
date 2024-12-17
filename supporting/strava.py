import requests
import json
import logging
from database.db import Connection
from datetime import datetime
from requests.exceptions import RequestException
import random
import time


formatter = logging.Formatter('[%(levelname)s] %(message)s')
log = logging.getLogger()
log.setLevel("INFO")

class Response:
    def __init__(self, status_code, reason, content, rate):
        self.status_code = status_code
        self.reason = reason
        self.reason = reason
        self.rate = rate

def retry_request(url, headers=None, max_retries=5, params=None, method='get', json=None):

    for attempt in range(max_retries + 1):
        try:
            if method == 'get':
                response = requests.get(url, headers=headers, params=params)
                if response.status_code == 429:
                    log.error(f"{response.status_code}: {response.reason}")
                    rate_used = [int(rate) for rate in response.headers['x-readratelimit-usage'].split(',')]
                    log.error(f"{rate_used[0]} / 100")
                    log.error(f"{rate_used[1]} / 1000")
                    return Response(response.status_code, response.reason, None, int(rate_used[1]))
                elif response.status_code == 404:
                    log.error(f"{response.status_code}: {response.reason}")
                    return Response(response.status_code, response.reason, None, 1)
                else:
                    response.raise_for_status()
                    log.info(f"{response.status_code}: {response.reason}")
                    return response
            if method == 'post':
                if json is None:
                    response = requests.post(url, headers=headers, params=params)
                else:
                    response = requests.post(url, headers=headers, params=params, json=json)
                if response.status_code == 429:
                    log.error(f"{response.status_code}: {response.reason}")
                    rate_used = [int(rate) for rate in response.headers['x-readratelimit-usage'].split(',')]
                    log.error(f"{rate_used[0]} / 100")
                    log.error(f"{rate_used[1]} / 1000")
                    return Response(response.status_code, response.reason, None, int(rate_used[1]))
                elif response.status_code == 404:
                    log.error(f"{response.status_code}: {response.reason}")
                    return Response(response.status_code, response.reason, None, 0)
                else:
                    response.raise_for_status()
                    log.info(f"{response.status_code}: {response.reason}")
                    return response

        except RequestException as e:
            if attempt < max_retries:
                backoff_time = 2 ** attempt + random.uniform(0, 1)
                log.warning(f"{e}")
                log.warning(f"Request failed. Retrying in {backoff_time:.2f} seconds... (retry attempt {attempt + 1}/{max_retries})")
                time.sleep(backoff_time)
            else:
                log.error(f"Request failed after {max_retries} retries")
                log.error(f"Error: {e}")
                return Response(500, e, None)


class Strava:
    @staticmethod
    def get_token(db):
        result = db.get_all(table='access_key')
        access_token = result[1]
        refresh_token = result[2]
        expire_date = result[3]
        client_id = result[4]
        client_secret = result[5]
        now = int(datetime.now().timestamp())
        delta = expire_date - now
        if delta < 1000:
            log.info("Access token has to be renewed")
            auth_url = "https://www.strava.com/oauth/token"
            params = {
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token"
            }

            log.info("Retrieving new access token")
            response = retry_request(auth_url, headers=None, method='post', params=params)
            if response.status_code != 200:
                exit()

            content = json.loads(response.content)
            new_values = {
                "access_token": content["access_token"],
                "refresh_token": content["refresh_token"],
                "expires_at": content["expires_at"]
            }
            log.info("Write access token to database")
            db.update(table='access_key', record_id=1, json_data=new_values)
            access_token = content["access_token"]


        return access_token

    def __init__(self, db):
        self.access_token = self.get_token(db)
        # self.access_token = "d572f0146bb08b673134f5d8a1735beda026edab"

    def getactvities(self, start_date, end_date, page=1, pagesize=200):
        log.info("Read list of activities")
        url = f"https://www.strava.com/api/v3/athlete/activities?before={end_date}&after={start_date}&page={page}&per_page={pagesize}"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)

    def activity(self, activity_id):
        log.info(f"Read details of activity with ID = {activity_id}")
        url = f"https://www.strava.com/api/v3/activities/{activity_id}"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)


        elif response.status_code != 200:
            return {}

        return json.loads(response.content)

    def activity_stream(self, activity_id):
        log.info(f"Read streams of activity with ID = {activity_id}")
        stream_types = ['time', 'altitude', 'heartrate', 'cadence', 'latlng']
        params = {
            'keys': ','.join(stream_types),
            'key_by_type': 'true'  # Organize data by stream type
        }
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams?keys="
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, params=params, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)


        elif response.status_code != 200:
            return []

        return json.loads(response.content)

    def activity_laps(self, activity_id):
        log.info(f"Read laps of activity with ID = {activity_id}")

        url = f"https://www.strava.com/api/v3/activities/{activity_id}/laps"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')

        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)


        elif response.status_code != 200:
            return []

        return json.loads(response.content)

    def dump(self, start_date, end_date, page_size=200):
        page = 0
        result_size = page_size
        result_set = []
        while result_size == page_size:
            page += 1
            content = self.getactvities(start_date=start_date, end_date=end_date, page=page, pagesize=page_size)
            result_size = len(content)
            log.info(f"Found {result_size} results on page {page}")
            for item in content:
                result_set.append(item)

        return result_set


    def getgear(self, gear_id):
        log.info(f"Read gear with id={gear_id}")
        url = f"https://www.strava.com/api/v3/gear/{gear_id}"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)


    def getclub(self, club_id):
        log.info(f"Read club with id={club_id}")
        url = f"https://www.strava.com/api/v3/clubs/{club_id}"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)

    def getclubactivities(self, club_id,  page=1, pagesize=200):
        log.info("Read list of activities")
        url = f"https://www.strava.com/api/v3/clubs/{club_id}/activities?page={page}&per_page={pagesize}"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)


    def athlete(self):
        log.info(f"Read athletes")
        url = f"https://www.strava.com/api/v3/athlete"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)

    def athletezones(self):
        log.info(f"Read athletes")
        url = f"https://www.strava.com/api/v3/athlete/zones"
        _headers = {
            "Authorization": f"Bearer {self.access_token}"
        }

        response = retry_request(url, headers=_headers, method='get')
        if response.status_code == 429:
            if response.rate >= 1000:
                log.info(f"Daily usage = {response.rate} >= 1000, so we are done for this day")
                exit()
            else:
                log.info(f"Daily usage = {response.rate} < 1000, so let's have a break for 15 minutes")
                time.sleep(15 * 60)
                response = retry_request(url, headers=_headers, method='get')
                return json.loads(response.content)

        elif response.status_code != 200:
            return []

        return json.loads(response.content)
