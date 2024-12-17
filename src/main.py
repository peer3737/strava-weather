import logging
import os
from datetime import datetime, timedelta
from database.db import Connection
import uuid
from supporting import aws
import math
from supporting.open_meteo import Weather


class CorrelationIdFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        # Generate a new correlation ID
        self.correlation_id = str(uuid.uuid4())

    def filter(self, record):
        # Add correlation ID to the log record
        record.correlation_id = self.correlation_id
        return True


# Logging formatter that includes the correlation ID
formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [Correlation ID: %(correlation_id)s] %(message)s')

# Set up the root logger
log = logging.getLogger()
log.setLevel("INFO")
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("botocore").setLevel(logging.WARNING)

# Remove existing handlers
for handler in log.handlers:
    log.removeHandler(handler)

# Add a new handler with the custom formatter
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

# Add the CorrelationIdFilter to the logger
correlation_filter = CorrelationIdFilter()
log.addFilter(correlation_filter)
weather_table = os.getenv('WEATHER_TABLE')


def calculate_wet_bulb(realtemp, rh):
    tw = realtemp * math.atan(0.151977 * math.pow((rh + 8.313659), 0.5)) + math.atan(realtemp + rh) - \
         math.atan(rh - 1.676331) + 0.00391838 * math.pow(rh, 1.5) * math.atan(0.023101 * rh) - 4.686035
    return round(tw, 1)


def haversine_afstand(lat1, lon1, lat2, lon2):
    """
    Berekent de hemelsbrede afstand tussen twee punten op aarde.

    Args:
    lat1: Latitude van het eerste punt in graden.
    lon1: Longitude van het eerste punt in graden.
    lat2: Latitude van het tweede punt in graden.
    lon2: Longitude van het tweede punt in graden.

    Returns:
    De afstand tussen de twee punten in kilometers.
    """
    # Straal van de aarde in kilometers
    r = 6371.0

    # Converteer graden naar radialen
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # Verschil in latitude en longitude
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Haversine formule
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Afstand in kilometers
    afstand = r * c
    return afstand


def lambda_handler(event, context):
    activity_id = event.get("activity_id")
    log.info(f"Start handling laps for activity {activity_id}")
    database_id = os.getenv('DATABASE_ID')
    database_settings = aws.dynamodb_query(table='database_settings', id=database_id)
    db_host = database_settings[0]['host']
    db_user = database_settings[0]['user']
    db_password = database_settings[0]['password']
    db_port = database_settings[0]['port']
    db = Connection(user=db_user, password=db_password, host=db_host, port=db_port, charset="utf8mb4")

    all_activities = db.get_specific(table='activity', where=f'id = {activity_id}', order_by_type='desc')
    act_counter = 0
    total_act = len(all_activities)
    for activity in all_activities:
        act_counter += 1
        activity_id = activity[0]
        start_date_time = activity[9]
        log.info(f'Handling activity {activity_id} ({act_counter}/{total_act})')

        activity_streams = db.get_specific(table='activity_streams', where=f'activity_id={activity_id}')[0]
        if activity_streams is None:
            continue
        if activity_streams[2] is None:
            continue
        if activity_streams[5] is None:
            continue
        activity_latlngs = activity_streams[5].split('],[')
        activity_latlngs = activity_latlngs[1:]
        activity_latlngs[len(activity_latlngs)-1] = activity_latlngs[len(activity_latlngs)-1][:-1]
        activity_times = activity_streams[2].split(',')
        percentage_size = 10
        percentage_step = math.floor(len(activity_latlngs) / percentage_size)
        measures = []
        for i in range(0, percentage_size):
            measure = activity_latlngs[i*percentage_step]
            measures.append(measure)

        date_times = []
        for activity_time in activity_times:
            date_times.append(start_date_time + timedelta(seconds=int(activity_time)))

        end_date_time = str(date_times[len(date_times)-1])

        all_measurements = {}
        for measure in measures:
            lat = measure.split(',')[0].strip()
            lon = measure.split(',')[1].strip()
            # print(f'lat = {lat}, lon = {lon}')
            weather = Weather(lat=lat, lon=lon)
            measured_weather = None
            try:
                measured_weather = weather.history(start_date=start_date_time.date(),
                                                   end_date=(datetime.strptime(end_date_time, '%Y-%m-%d %H:%M:%S') +
                                                             timedelta(days=1)).date())

            except Exception as e:
                log.error(end_date_time)
                log.error(e)
                exit()
            # print(measured_weather)
            for i in range(0, len(measured_weather['hourly']['time'])):
                time = measured_weather['hourly']['time'][i]
                temp = measured_weather['hourly']['temperature_2m'][i]
                humidity = measured_weather['hourly']['relative_humidity_2m'][i]
                air_pressure = measured_weather['hourly']['surface_pressure'][i]
                try:
                    wet_bulb = calculate_wet_bulb(float(temp), float(humidity))
                except Exception as e:
                    wet_bulb = 9999
                    log.info(activity_id)
                    log.info(measured_weather)
                    exit()
                apparent_temp = measured_weather['hourly']['apparent_temperature'][i]
                wind_speed = measured_weather['hourly']['wind_speed_10m'][i]
                wind_direction = measured_weather['hourly']['wind_direction_10m'][i]
                weather_code = measured_weather['hourly']['weather_code'][i]
                if measured_weather['hourly']['time'][i] not in all_measurements:
                    all_measurements[time] = {}

                latitude = measured_weather['latitude']
                longitude = measured_weather['longitude']
                lanlon = f'{latitude},{longitude}'
                if lanlon not in all_measurements[time]:
                    all_measurements[time][lanlon] = {
                        "temp": temp,
                        "weather_code": weather_code,
                        "wet_bulb": wet_bulb,
                        "wind_direction": wind_direction,
                        "wind_speed": wind_speed,
                        "apparent_temp": apparent_temp,
                        "humidity": humidity,
                        "air_pressure": air_pressure
                    }

        # print(json.dumps(all_measurements))

        temp = []
        wet_bulb = []
        wind_direction = []
        wind_speed = []
        apparent_temp = []
        humidity = []
        air_pressure = []

        # activity_latlngs = ['51.955341, 4.212948']
        # activity_times = [datetime(2024, 7, 20, 14, 30, 2)]
        for t in range(0, len(activity_times)):
            correction = 0
            temp.append(0)
            wet_bulb.append(0)
            wind_direction.append(0)
            wind_speed.append(0)
            apparent_temp.append(0)
            humidity.append(0)
            air_pressure.append(0)
            while len(activity_latlngs) <= t-correction:
                correction += 1

            lat_check = float(activity_latlngs[t-correction].split(',')[0].strip())
            lon_check = float(activity_latlngs[t-correction].split(',')[1].strip())
            datetime_compare = date_times[t]
            # lat_check = 51.955341
            # lon_check = 4.212948
            # datetime_compare = datetime(2024, 7, 20, 14, 30, 2)
            start_of_hour = datetime_compare.replace(minute=0, second=0, microsecond=0)
            start_of_next_hour = start_of_hour + timedelta(hours=1)
            diff_start_hour = (datetime_compare - start_of_hour).total_seconds()
            diff_next_hour = (start_of_next_hour - datetime_compare).total_seconds()

            start_hour_contribution = (3600 - diff_start_hour) / 3600
            next_hour_contribution = (3600 - diff_next_hour) / 3600
            datetime_start = start_of_hour.strftime('%Y-%m-%dT%H:%M')
            datetime_end = start_of_next_hour.strftime('%Y-%m-%dT%H:%M')

            datetimes = [datetime_start, datetime_end]
            contributions = [start_hour_contribution, next_hour_contribution]
            data = all_measurements

            for i in range(0, len(datetimes)):
                contribution = contributions[i]
                datetimevalue = datetimes[i]

                distance_product = 1
                distances = []
                for latlon in data[datetimevalue]:
                    lat = float(latlon.split(',')[0])
                    lon = float(latlon.split(',')[1])
                    distance = haversine_afstand(lat_check, lon_check, lat, lon)
                    distances.append(distance)
                    distance_product *= distance

                temp_total = 0
                wet_bulb_total = 0
                wind_direction_total = 0
                wind_speed_total = 0
                apparent_temp_total = 0
                humidity_total = 0
                air_pressure_total = 0
                total_parts = 0
                counter = 0
                for latlon in data[datetimevalue]:
                    part = distance_product / distances[counter]
                    temp_total += data[datetimevalue][latlon]['temp'] * part

                    wet_bulb_total += data[datetimevalue][latlon]['wet_bulb'] * part
                    wind_direction_total += data[datetimevalue][latlon]['wind_direction'] * part
                    wind_speed_total += data[datetimevalue][latlon]['wind_speed'] * part
                    apparent_temp_total += data[datetimevalue][latlon]['apparent_temp'] * part
                    humidity_total += data[datetimevalue][latlon]['humidity'] * part
                    air_pressure_total += data[datetimevalue][latlon]['air_pressure'] * part

                    total_parts += part

                    counter += 1

                temp[t] += temp_total/total_parts*contribution
                wet_bulb[t] += wet_bulb_total/total_parts*contribution
                wind_direction[t] += wind_direction_total/total_parts*contribution
                wind_speed[t] += wind_speed_total/total_parts*contribution
                apparent_temp[t] += apparent_temp_total/total_parts*contribution
                humidity[t] += humidity_total/total_parts*contribution
                air_pressure[t] += air_pressure_total/total_parts*contribution

            temp[t] = str(round(temp[t], 1))
            wet_bulb[t] = str(round(wet_bulb[t], 1))
            wind_direction[t] = str(round(wind_direction[t], 1))
            wind_speed[t] = str(round(wind_speed[t], 1))
            apparent_temp[t] = str(round(apparent_temp[t], 1))
            humidity[t] = str(round(humidity[t], 1))
            air_pressure[t] = str(round(air_pressure[t], 1))

        datainput = {
            "activity_id": activity_id,
            "temp": ', '.join(temp),
            "wet_bulb": ', '.join(wet_bulb),
            "wind_direction": ', '.join(wind_direction),
            "wind_speed": ', '.join(wind_speed),
            "apparent_temp": ', '.join(apparent_temp),
            "humidity": ', '.join(humidity),
            "air_pressure": ', '.join(air_pressure)
        }

        db.insert(table=weather_table, json_data=datainput)

#
#
# weercode = {
#     0: "Onbewolkt",
#     1: "Lichtbewolkt",
#     2: "Halfbewolkt",
#     3: "Zwaar bewolkt",
#     45: "Mist",
#     48: "Rijpmist",
#     51: "Lichte motregen",
#     53: "Motregen",
#     55: "Zware motregen",
#     56: "Lichte ijsmotregen",
#     57: "IJsmotregen",
#     61: "Lichte regen",
#     63: "Regen",
#     65: "Zware regen",
#     66: "Lichte ijsregen",
#     67: "Zware ijsregen",
#     71: "Lichte sneeuwval",
#     73: "Sneeuwval",
#     75: "Zware sneeuwval",
#     77: "Korrelsneeuw",
#     80: "Lichte regenbuien",
#     81: "Regenbuien",
#     82: "Zware regenbuien",
#     85: "Lichte sneeuwbuien",
#     86: "Zware sneeuwbuiten",
#     95: "Onweer",
#     96: "Onweer met lichte hagel",
#     99: "Onweer met zware hagel"
# }
#
# def get_closest_wind_direction(degrees):
#     """
#     Finds the closest wind direction in the windrichting dictionary for a given degree value.
#
#     Args:
#     degrees: The input degree value (between 0 and 360).
#
#     Returns:
#     The closest wind direction string.
#     """
#     windrichting = {
#         "N": 0,
#         "NNO": 22.5,
#         "NO": 45,
#         "ONO": 67.5,
#         "O": 90,
#         "OZO": 112.5,
#         "ZO": 135,
#         "ZZO": 157.5,
#         "Z": 180,
#         "ZZW": 202.5,
#         "ZW": 225,
#         "WZW": 247.5,
#         "W": 270,
#         "WNW": 292.5,
#         "NW": 315,
#         "NNW": 337.5
#     }
#
#
#
#     # Ensure degrees is within the valid range
#     degrees = degrees % 360
#     # Calculate differences between the input degrees and all directions
#     differences = {k: abs(v - degrees) for k, v in windrichting.items()}
#     # Find the key with the minimum difference
#     return min(differences, key=differences.get)
#
# def get_beaufort_number(windspeed_kmh):
#     """
#     Calculates the Beaufort number based on wind speed in km/h.
#
#     Args:
#     windspeed_kmh: Wind speed in kilometers per hour.
#
#     Returns:
#     The corresponding Beaufort number (integer).
#     """
#     if windspeed_kmh < 1:
#         return 0  # Calm
#     elif windspeed_kmh < 5.5:
#         return 1  # Light air
#     elif windspeed_kmh < 11:
#         return 2  # Light breeze
#     elif windspeed_kmh < 19:
#         return 3  # Gentle breeze
#     elif windspeed_kmh < 28:
#         return 4  # Moderate breeze
#     elif windspeed_kmh < 38:
#         return 5  # Fresh breeze
#     elif windspeed_kmh < 49:
#         return 6  # Strong breeze
#     elif windspeed_kmh < 61:
#         return 7  # Near gale
#     elif windspeed_kmh < 74:
#         return 8  # Gale
#     elif windspeed_kmh < 88:
#         return 9  # Strong gale
#     elif windspeed_kmh < 102:
#         return 10  # Storm
#     elif windspeed_kmh < 117:
#         return 11  # Violent storm
#     else:
#         return 12  # Hurricane force
#
# weather = Weather(lat=51.95991809581691, lon=4.214915919364949)
#
# measured_weather = weather.history(start_date="2024-07-20", end_date="2024-07-20")
#
# type = 'hourly'
# print(measured_weather)
#
#
# for i in range(0, len(measured_weather[type]['time'])):
#     if measured_weather[type]['relative_humidity_2m'][i] > 0 and measured_weather[type]['temperature_2m'][i] > 1:
#         print(f"{measured_weather[type]['time'][i]}: {measured_weather[type]['temperature_2m'][i]}°C - "
#               f"{measured_weather[type]['apparent_temperature'][i]}°C - "
#               f"{measured_weather[type]['dew_point_2m'][i]} - "
#               f"{measured_weather[type]['relative_humidity_2m'][i]}% - "
#               f"{measured_weather[type]['surface_pressure'][i]} - "
#               f"{calculate_wet_bulb(measured_weather[type]['temperature_2m'][i], measured_weather[type]['relative_humidity_2m'][i])} - "
#               f"{get_closest_wind_direction(measured_weather[type]['wind_direction_10m'][i])}{get_beaufort_number(measured_weather[type]['wind_speed_10m'][i])} - "
#               f"{weercode[measured_weather[type]['weather_code'][i]]}")
