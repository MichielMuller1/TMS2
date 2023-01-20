#!/usr/bin/env python3

# TMS2 Raspberry Pi (Server) back-end script.
# Dit script ............

from datetime import datetime
import paho.mqtt.client as mqtt
import json, requests, threading, signal, sys, time, math

def on_message(client, userdata, message):
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "sensorId": payload["sensorId"], "value": payload["sensorValue"]}
    # Payload verzenden
    headers = {'Content-type': 'application/json'}
    xurl = "https://hooyberghs-api.azurewebsites.net/api/sensorvalue/"
    response = requests.post(xurl, json=data, headers=headers)
    # Error code
    if response.status_code in (200, 201):
        print("Succesvol: Payload verzenden naar database gelukt. (DiepteSensor: " + payload["sensorId"] + ")")
    else:
        print("Mislukt: Payload verzenden naar database mislukt! (Dieptesensor: " + payload["sensorId"] + ")")

def on_message2(client, userdata, message):   
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "value": payload["stroomValue"], "pumpId": payload["actorId"]}
    # Payload verzenden
    headers = {'Content-type': 'application/json'}
    xurl = "https://hooyberghs-api.azurewebsites.net/api/pumpvalue/"
    response = requests.post(xurl, json=data, headers=headers)
    # Error code
    if response.status_code in (200, 201):
        print("Succesvol: Payload verzenden naar database gelukt. (Stroommeting: " + payload["actorId"] + ")")
    else:
        print("Mislukt: Payload verzenden naar database mislukt! (Stroommeting: " + payload["actorId"] + ")")
        
        
        
def led_range(percentage, total_leds):
    return math.ceil((percentage/100) * total_leds)        
        
def checkWijzigingenPomp():
    api_url = "https://hooyberghs-api.azurewebsites.net/api/pump"
    response = requests.get(api_url)
    data = json.loads(response.text)
    previous_input_values = {}
    for pump in data:
        previous_input_values[pump["id"]] = pump["inputValue"]

    while True:
        response = requests.get(api_url)
        data = json.loads(response.text)
        for pump in data:
            current_input_value = pump["inputValue"]
            if current_input_value != previous_input_values[pump["id"]]:
                
                xrange_value = led_range(current_input_value, 30)
                
                
                mqtt_topic = f"werf/actoren/actor_{pump['id']}"
                
                xmessage = str({"led": str(xrange_value), "input": str(current_input_value)})
                # xmessage = str(xrange_value) + " " +str(current_input_value)
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                print(f"Wijziging: {pump['name']} inputValue: {current_input_value}")
                previous_input_values[pump["id"]] = current_input_value
        time.sleep(4)
        
def checkWijzigingenPompOld():
    api_url = "https://hooyberghs-api.azurewebsites.net/api/oldpump"
    response = requests.get(api_url)
    data = json.loads(response.text)
    previous_input_values = {}
    for pump in data:
        previous_input_values[pump["id"]] = pump["inputValue"]

    while True:
        response = requests.get(api_url)
        data = json.loads(response.text)
        for pump in data:
            current_input_value = pump["inputValue"]
            if current_input_value != previous_input_values[pump["id"]]:
                mqtt_topic = f"werf/actoren/old/actor_{pump['id']}"
                client.publish(mqtt_topic, str(current_input_value))
                print(f"Wijziging: {pump['name']} inputValue: {current_input_value}")
                previous_input_values[pump["id"]] = current_input_value
        time.sleep(4)      
        
        
def signal_handler(sig, frame):
    client.disconnect()
    client2.disconnect()
    client3.disconnect()
    client4.disconnect()
    print('Exited!')
    quit()

# Set up the MQTT client (sensors)
client = mqtt.Client()
client.connect("localhost", 1883)
client.subscribe("werf/sensoren/#")
client.on_message = on_message

# Set up the MQTT client (stroommetingen)
client2 = mqtt.Client()
client2.connect("localhost", 1883)
client2.subscribe("werf/actoren/+/stroom")
client2.on_message = on_message2

# Set up the MQTT client (Wijzigingen pomp)
client3 = mqtt.Client()
client3.connect("localhost", 1883)

# Set up the MQTT client (Wijzigingen oude pomp)
client4 = mqtt.Client()
client4.connect("localhost", 1883)

# Create and start threads
thread1 = threading.Thread(target=client.loop_forever)
thread2 = threading.Thread(target=client2.loop_forever)
thread3 = threading.Thread(target=checkWijzigingenPomp)
thread4 = threading.Thread(target=checkWijzigingenPompOld)

thread1.start()
thread2.start()
thread3.start()
thread4.start()
    
signal.signal(signal.SIGINT, signal_handler)

