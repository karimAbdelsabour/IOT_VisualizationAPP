import influxdb_client
import os
import time
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import random

token = os.environ.get("INFLUXDB_TOKEN")
org = "AssiutUniversity"
url = "http://localhost:8086"


write_client = InfluxDBClient(url=url, token=token, org=org)


write_api = write_client.write_api(write_options=SYNCHRONOUS)

# Define the bucket
bucket = "Sensors"


mqtt_broker = '0.0.0.0'  
mqtt_port = 1883           
mqtt_topic = "sensor_data" 


mqtt_client = mqtt.Client()


mqtt_client.connect(mqtt_broker, mqtt_port, 60)


try:
    while True:

        gas_value = random.uniform(0, 100) 
        humidity_value = random.uniform(0, 100) 
        temperature_value = random.uniform(-10, 35)  
        

        gas_point = (
            Point("gas_measurement")
            .tag("location", "office")  
            .field("gas", gas_value)
        )
        humidity_point = (
            Point("humidity_measurement")
            .tag("location", "office")  
            .field("humidity", humidity_value)
        )
        temperature_point = (
            Point("temperature_measurement")
            .tag("location", "office")  
            .field("temperature", temperature_value)
        )
        

        write_api.write(bucket=bucket, org=org, record=gas_point)
        write_api.write(bucket=bucket, org=org, record=humidity_point)
        write_api.write(bucket=bucket, org=org, record=temperature_point)
        

        mqtt_message = (
            f"gas_measurement,location=office gas={gas_value}\n"
            f"humidity_measurement,location=office humidity={humidity_value}\n"
            f"temperature_measurement,location=office temperature={temperature_value}"
        )
        mqtt_client.publish(mqtt_topic, mqtt_message)
        

        time.sleep(1)

except KeyboardInterrupt:

    print("Terminating data generation...")

finally:

    mqtt_client.disconnect()
    write_client.close()
    print("Disconnected from MQTT and closed InfluxDB client.")
