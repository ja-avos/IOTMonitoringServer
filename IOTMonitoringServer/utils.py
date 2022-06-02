from django.utils.timezone import activate
from receiver.models import (
    Measurement,
    Station,
    User,
    Data,
    State,
    City,
    Country,
    Location,
)
from django.contrib.auth.models import User as AuthUser
from django.db.models import Max, Sum
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import datetime as datetimelib
import ssl
import random
import time
import os
import requests

from . import settings

"""
Registra los usuarios que están en users.pwd (en la carpeta raíz del proyecto) en el sistema.
Por cada usuario, el sistema crea el objeto User, luego crea un usuario en el sistema de 
autenticación de Django con usuario login y la contraseña MQTT descrita en el archivo users.pwd.
"""


def register_users():
    registered_count = 0
    registering_count = 0
    error_count = 0

    print("Utils: Registering users...")

    with open(settings.BASE_DIR / "users.pwd", "r") as users_file:
        lines = users_file.readlines()
        for line in lines:
            [login, passwd] = line.split(":")
            login = login.strip()
            passwd = passwd.strip()
            try:
                user = AuthUser.objects.get(username=login)
                print(f"User {login} already registered")
                registered_count += 1
            except AuthUser.DoesNotExist:
                AuthUser.objects.create_user(
                    login, login + "@mail.com", passwd)
                registering_count += 1
            except Exception as e:
                print(f"Error registering u: {login}. Error: {e}")
                error_count += 1
        print("Utils: Users registered.")
        print(
            f"Utils: Already users: {registered_count}, \
                 Registered users: {registering_count}, \
                     Error use rs: {error_count}, Total success: \
                         {registered_count+ registering_count}"
        )


"""
Crea una medición en la base de datos.
Se usa para la importación de los datos desde CSV.
"""


def saveMeasure(user: str, city: str, date: datetime, variable: str, measure: float):
    from realtimeGraph.views import (
        create_data_with_date,
        get_or_create_location,
        get_or_create_location_only_city,
        get_or_create_measurement,
        get_or_create_user,
        get_or_create_station,
        create_data,
    )

    try:
        user_obj = get_or_create_user(user)
        location_obj = get_or_create_location_only_city(city)
        unit = "°C" if str(variable).lower() == "temperatura" else "%"
        variable_obj = get_or_create_measurement(variable, unit)
        sensor_obj = get_or_create_station(user_obj, location_obj)
        create_data_with_date(measure, sensor_obj, variable_obj, date)
    except Exception as e:
        print("ERROR saving measure: ", e)


"""
Función para importar los datos del archivo input.csv en la raíz del proyecto.
"""


def loadCSV():
    filepath = settings.BASE_DIR / "input.csv"
    with open(filepath, "r") as data_file:
        lines = data_file.readlines()
        lon = len(lines)
        count = 1
        print("CSV length: ", lon)
        for line in lines[1:]:
            print("Reg ", count, "of", lon)
            usuario, ciudad, fecha, variable, medicion = line.split(",")
            date = datetime.strptime(fecha, "%Y-%m-%d %H:%M:%S")
            saveMeasure(
                user=usuario,
                city=ciudad,
                date=date,
                variable=variable,
                measure=float(medicion),
            )


"""
Función auxiliar para obtener la última linea de un archivo.
"""


def getLastLine(file):
    try:  # catch OSError in case of a one line file
        file.seek(-2, os.SEEK_END)
        while file.read(1) != b"\n":
            file.seek(-2, os.SEEK_CUR)
    except OSError:
        file.seek(0)
    last_line = file.readline().decode()
    return last_line


"""
Función para generar datos ficticios para poblar el sistema.
Se usa para hacer pruebas de carga.
"""


def generateMockData(quantity: int = 500000):
    from receiver.utils import create_data, get_coordinates

    print("Starting generation of {} data...".format(quantity))

    query_len = Data.objects.aggregate(Sum("length"))
    print("Query len:", query_len)
    data_len = query_len["length__sum"] or 0

    print("Data in database:", data_len)

    if data_len > quantity:
        print("Mock data already generated.")
        return

    measure1, created = Measurement.objects.get_or_create(
        name="Temperatura", unit="°C")
    measure2, created = Measurement.objects.get_or_create(
        name="Humedad", unit="%")

    users = AuthUser.objects.all()

    cities_raw = [
        ["Bogotá", "Cundinamarca", "Colombia"],
        ["Medellín", "Antioquia", "Colombia"],
        ["Barranquilla", "Atlántico", "Colombia"],
        ["Cartagena", "Bolívar", "Colombia"],
        ["Cali", "Valle", "Colombia"],
    ]

    locations = []

    for city in cities_raw:
        city_name, department, country_name = city
        city, created = City.objects.get_or_create(name=city_name)
        state, created = State.objects.get_or_create(name=department)
        country, created = Country.objects.get_or_create(name=country_name)

        lat, lng = get_coordinates(city_name, department, country_name)

        location, created = Location.objects.get_or_create(
            city=city, state=state, country=country, lat=lat, lng=lng
        )
        locations.append(location)

    stations = []

    for user in users:
        max_stations = random.randint(1, 3)
        qty = 0
        while qty < max_stations:
            location = random.choice(locations)
            station, created = Station.objects.get_or_create(
                user=user, location=location
            )
            stations.append(station)
            qty += 1

    data_per_day = 1000
    initial_date = datetime.now() - relativedelta(months=1)
    interval = ((24 * 60 * 60 * 1000) / data_per_day) // 1

    print("Init date: ", initial_date)
    print("Data per day: ", data_per_day)
    print("Interval (milliseconds):", interval)

    stations = Station.objects.all()
    measures = Measurement.objects.all()
    print("Total stations:", len(stations))
    print("Total measures:", len(measures))

    if data_len > 0:
        cd_query = Data.objects.aggregate(Max("base_time"))
        current_date = cd_query["base_time__max"]
        current_date = current_date + timedelta(hours=1)
    else:
        current_date = initial_date

    count = data_len if data_len != None else 0

    while count <= quantity and current_date < datetime.now():
        rand_station = random.randint(0, len(stations) - 1)
        rand_measure = random.randint(0, len(measures) - 1)
        station = stations[rand_station]
        measure = measures[rand_measure]
        data = random.random() * 40
        create_data(data, station, measure, current_date)
        print("Data created:", count, current_date.timestamp())
        count += 1
        current_date += timedelta(milliseconds=interval)

    print("Finished. Total data:", count, "Last date:", current_date)
