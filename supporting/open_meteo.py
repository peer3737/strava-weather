import requests
import logging
from datetime import datetime


formatter = logging.Formatter('[%(levelname)s] %(message)s')
log = logging.getLogger()
log.setLevel("INFO")


class Response:
    def __init__(self, status_code, reason, rate):
        self.status_code = status_code
        self.reason = reason
        self.reason = reason
        self.rate = rate


class Weather:
    def __init__(self, lon, lat):
        self.long = lon
        self.lat = lat

    def get(self, date=None):
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": self.lat,
            "longitude": self.long,
            "minutely_15": ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "weather_code", "wind_speed_10m", "wind_direction_10m", "dew_point_2m"],
            "start_date": date,
            "end_date": date
        }
        response = requests.get(url, params=params)

        return response.json()

    def history(self, start_date, end_date):
        current_date = datetime.now().date()

        difference = (current_date - start_date).days
        if difference <= 6:
            url = "https://api.open-meteo.com/v1/forecast"
        else:
            url = "https://archive-api.open-meteo.com/v1/archive"

        params = {
            "latitude": self.lat,
            "longitude": self.long,
            "hourly": ["temperature_2m", "apparent_temperature", "relative_humidity_2m", "weather_code", "wind_speed_10m", "wind_direction_10m", "dew_point_2m", "surface_pressure"],
            "start_date": start_date,
            "end_date": end_date
        }

        response = requests.get(url, params=params)
        # print(response.content)
        # print(response.headers)
        # print(response.status_code)
        # print(response.reason)
        return response.json()
