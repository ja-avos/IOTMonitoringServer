from argparse import ArgumentError
import enum
import ssl
from django.db.models import Avg
from datetime import timedelta, datetime
from receiver.models import Data, Measurement
import paho.mqtt.client as mqtt
import schedule
import time
import enum
from django.conf import settings

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, settings.MQTT_USER_PUB)

class MessageType(enum.Enum):
    ALERT = "alert"
    NORMAL = "message"

class Message():
    def __init__(self, msg_type: MessageType, payload: str):
        self.msg_type = msg_type
        self.payload = payload

def check_min_max_overall():
    """
    Retorna una lista con diccionarios con el nombre de la variable, su valor promedio, su valor mínimo y su valor máximo, en caso de
    que el valor esté fuera de los límites definidos en la base de datos.
    """

    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    aggregation = data.annotate(check_value=Avg('avg_value')) \
        .select_related('station', 'measurement') \
        .select_related('station__user', 'station__location') \
        .select_related('station__location__city', 'station__location__state',
                        'station__location__country') \
        .values('check_value', 'station__user__username',
                'measurement__name',
                'measurement__max_value',
                'measurement__min_value',
                'station__location__city__name',
                'station__location__state__name',
                'station__location__country__name')
    
    alerts = []
    for item in aggregation:
        variable = item["measurement__name"]
        max_value = item["measurement__max_value"] or 0
        min_value = item["measurement__min_value"] or 0

        country = item['station__location__country__name']
        state = item['station__location__state__name']
        city = item['station__location__city__name']
        user = item['station__user__username']

        if item["check_value"] > max_value or item["check_value"] < min_value:
            alerts.append({
                "country": country,
                "state": state,
                "city": city,
                "user": user,
                "message": Message(
                    MessageType.ALERT,
                    "{} {} {} {}".format(variable, min_value, max_value, item["check_value"])
                )
            })

def check_fires():
    """
    Retorna una lista con diccionarios con pais, estado, ciudad, usuario y mensaje de alerta
    en caso de que se detecte una posible condición de incendio.
    Para detectar esta condición, se verifica por cada estación de medición,
    si las últimas 5 temperaturas registradas han crecido y las últimas 5 humedades han disminuido.
    """

    data = Data.objects.filter(
        base_time__gte=datetime.now() - timedelta(hours=1))
    
    stations = set(data.values_list('station', flat=True))

    alerts = []
    for station in stations:
        station_data = data.filter(station=station).select_related('measurement').order_by('-base_time')[:5]
        temperature_data = [d for d in station_data if d.measurement.name.lower() == "temperature"]
        humidity_data = [d for d in station_data if d.measurement.name.lower() == "humidity"]
        if len(temperature_data) == 5 and len(humidity_data) == 5:
            temp_increasing = all(temperature_data[i].avg_value < temperature_data[i+1].avg_value for i in range(4))
            humidity_decreasing = all(humidity_data[i].avg_value > humidity_data[i+1].avg_value for i in range(4))
            if temp_increasing and humidity_decreasing:
                station_obj = station_data[0].station
                country = station_obj.location.country.name
                state = station_obj.location.state.name
                city = station_obj.location.city.name
                user = station_obj.user.username
                alerts.append({
                    "country": country,
                    "state": state,
                    "city": city,
                    "user": user,
                    "message": Message(
                        MessageType.ALERT,
                        "INCENDIO"
                    )
                })

    return alerts

def send_message(message: Message, country: str, state: str, city: str, user: str):
    topic = '{}/{}/{}/{}/in'.format(country, state, city, user)
    print(datetime.now(), "Sending message to {}: {}".format(topic, message.payload))
    client.publish(topic, message.payload)

def analyze_data():
    # Consulta todos los datos de la última hora, los agrupa por estación y variable
    # Compara el promedio con los valores límite que están en la base de datos para esa variable.
    # Si el promedio se excede de los límites, se envia un mensaje de alerta.

    print("Calculando alertas...")

    alerts = []

    # alerts += check_min_max_overall()

    alerts += check_fires()

    for item in alerts:
        send_message(item["message"], item["country"], item["state"], item["city"], item["user"])

    print(alerts, "alertas enviadas")


def on_connect(client, userdata, flags, rc):
    '''
    Función que se ejecuta cuando se conecta al bróker.
    '''
    print("Conectando al broker MQTT...", mqtt.connack_string(rc))


def on_disconnect(client: mqtt.Client, userdata, rc):
    '''
    Función que se ejecuta cuando se desconecta del broker.
    Intenta reconectar al bróker.
    '''
    print("Desconectado con mensaje:" + str(mqtt.connack_string(rc)))
    print("Reconectando...")
    client.reconnect()


def setup_mqtt():
    '''
    Configura el cliente MQTT para conectarse al broker.
    '''

    print("Iniciando cliente MQTT...", settings.MQTT_HOST, settings.MQTT_PORT)
    global client
    try:
        client = mqtt.Client(settings.MQTT_USER_PUB)
        client.on_connect = on_connect
        client.on_disconnect = on_disconnect

        if settings.MQTT_USE_TLS:
            client.tls_set(ca_certs=settings.CA_CRT_PATH,
                           tls_version=ssl.PROTOCOL_TLSv1_2, cert_reqs=ssl.CERT_NONE)

        client.username_pw_set(settings.MQTT_USER_PUB,
                               settings.MQTT_PASSWORD_PUB)
        client.connect(settings.MQTT_HOST, settings.MQTT_PORT)

    except Exception as e:
        print('Ocurrió un error al conectar con el bróker MQTT:', e)


def start_cron():
    '''
    Inicia el cron que se encarga de ejecutar la función analyze_data cada 5 minutos.
    '''
    print("Iniciando cron...")
    schedule.every(2).minutes.do(analyze_data)
    print("Servicio de control iniciado")
    while 1:
        schedule.run_pending()
        time.sleep(1)
