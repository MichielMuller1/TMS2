#!/usr/bin/env python3

# TMS2 Raspberry Pi (Server) back-end script.
# Dit script ............

from datetime import datetime
import paho.mqtt.client as mqtt
import json, requests, threading, signal, sys, time, math, os

# --

#Global vars
xsleepx = 3
zero_sent_lock = threading.Lock()
zero_sent = {}
zero_sent_lock2 = threading.Lock()
zero_sent2 = {}  

def reset_zero_sent():
    global zero_sent
    with zero_sent_lock:
        zero_sent = {}
            
def reset_zero_sent2():
    global zero_sent2
    with zero_sent_lock2:
        zero_sent2 = {}
        
def led_range(percentage, total_leds):
    return math.ceil((percentage / 100) * total_leds)

def signal_handler(sig, frame):
    client.disconnect()
    client2.disconnect()
    client3.disconnect()
    exit(0)
    print('Exited!')

# --

def checkDiepteSensor(client, userdata, message):
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

def checkStroomMetingWaterFlow(client, userdata, message):
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "value": round(payload["stroomValue"], 2), "pumpId": payload["actorId"], "flowRate": round(payload["waterflow"], 3)}
    # Error check
    api_urlById = f'https://hooyberghs-api.azurewebsites.net/api/pump/{payload["actorId"]}'
    errorResponse = requests.get(api_urlById)
    object = json.loads(errorResponse.content)
    if object["isDefective"] == False:
        reset_zero_sent()
        # Payload verzenden
        headers = {'Content-type': 'application/json'}
        xurl = "https://hooyberghs-api.azurewebsites.net/api/pumpvalue/"
        response = requests.post(xurl, json=data, headers=headers)
        # Error code
        if response.status_code in (200, 201):
            print("Succesvol: Payload verzenden naar database gelukt. (Stroommeting: " + "pump" + payload["actorId"] + ")" 
                  , round(payload["stroomValue"], 2)," mA" +" flowRate:", round(payload["waterflow"], 3), "m³/s")
        else:
            print("Mislukt: Payload verzenden naar database mislukt! (Stroommeting: " + payload["actorId"] + ")")
    else:
        with zero_sent_lock:
            if payload["actorId"] not in zero_sent:
                data = {"date": timestamp, "value": 0, "pumpId": payload["actorId"], "flowRate": 0}
                headers = {'Content-type': 'application/json'}
                api_url = "https://hooyberghs-api.azurewebsites.net/api/pumpvalue/"
                response = requests.post(api_url, json=data, headers=headers)
                print("stroommeting stuk, gestopt met zenden.")
                zero_sent[payload["actorId"]] = True
              
def checkStroomMetingWaterFlowOld(client, userdata, message):
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "value": round(payload["stroomValue"], 2), "oldPumpId": payload["actorId"], "flowRate": round(payload["waterflow"], 3)}

    # Error check
    api_urlById = f'https://hooyberghs-api.azurewebsites.net/api/oldpump/{payload["actorId"]}'
    errorResponse = requests.get(api_urlById)
    object = json.loads(errorResponse.content)
    if object["isDefective"] == False:
        reset_zero_sent2()
        # Payload verzenden
        headers = {'Content-type': 'application/json'}
        xurl = "https://hooyberghs-api.azurewebsites.net/api/oldpumpvalue/"
        response = requests.post(xurl, json=data, headers=headers)
        # Error code
        if response.status_code in (200, 201):
            print("Succesvol: Payload verzenden naar database gelukt. (Stroommeting: " + "old_pump" + payload["actorId"] + ") " 
                  + str(payload["stroomValue"]) + " mA" +" flowRate:", round(payload["waterflow"], 3), "m³/s")
        else:
            print("Mislukt: Payload verzenden naar database mislukt! (Stroommeting: " + "old_pump" + payload["actorId"] + ")")
    else:
        with zero_sent_lock2:
            if payload["actorId"] not in zero_sent2:
                data = {"date": timestamp, "value": 0, "oldPumpId": payload["actorId"], "flowRate": 0}
                headers = {'Content-type': 'application/json'}
                api_url = "https://hooyberghs-api.azurewebsites.net/api/oldpumpvalue/"
                response = requests.post(api_url, json=data, headers=headers)
                print("stroommeting old stuk, gestopt met zenden.")
                zero_sent2[payload["actorId"]] = True

def checkPomp():
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
                xrange_input = led_range(current_input_value, 255)

                mqtt_topic = f"werf/actoren/actor_{pump['id']}"

                xmessage = str({"led": str(xrange_value), "input": str(xrange_input)})
                # xmessage = str(xrange_value) + " " +str(current_input_value)
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                print(f"Wijziging: {pump['name']} inputValue: {current_input_value}")
                previous_input_values[pump["id"]] = current_input_value

        time.sleep(xsleepx)

def checkPompOud():
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
                
        time.sleep(xsleepx)

def checkErrorSensor():
    api_url = "https://hooyberghs-api.azurewebsites.net/api/sensor"
    response = requests.get(api_url)
    data = json.loads(response.text)
    previous_input_values = {}
    errors = 0
    for sensor in data:
        previous_input_values[sensor["id"]] = sensor["isDefective"]
        if sensor["isDefective"] == True:
            errors += 1
    while True:
        response = requests.get(api_url)
        data = json.loads(response.text)
        for sensor in data:
            current_input_value = sensor["isDefective"]
            if current_input_value != previous_input_values[sensor["id"]]:
                if current_input_value == True:
                    errors += 1
                else:
                    errors -= 1
                mqtt_topic = "werf/error"
                xmessage = str({"name": "sensor", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                # print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[sensor["id"]] = current_input_value

        time.sleep(xsleepx)
        # print("sensor errors -> ", errors)

def checkErrorPump():
    api_url = "https://hooyberghs-api.azurewebsites.net/api/pump"
    response = requests.get(api_url)
    data = json.loads(response.text)
    previous_input_values = {}
    errors = 0
    for pomp in data:
        previous_input_values[pomp["id"]] = pomp["isDefective"]
        if pomp["isDefective"] == True:
            errors += 1
    while True:
        response = requests.get(api_url)
        data = json.loads(response.text)
        for pomp in data:
            current_input_value = pomp["isDefective"]
            if current_input_value != previous_input_values[pomp["id"]]:
                if current_input_value == True:
                    errors += 1
                else:
                    errors -= 1
                mqtt_topic = f"werf/error"
                xmessage = str({"name": "pump", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[pomp["id"]] = current_input_value

        time.sleep(xsleepx)
        # print("pomp errors -> ", errors)

def checkErrorOldPump():
    api_url = "https://hooyberghs-api.azurewebsites.net/api/oldpump"
    response = requests.get(api_url)
    data = json.loads(response.text)
    previous_input_values = {}
    errors = 0
    for pomp in data:
        previous_input_values[pomp["id"]] = pomp["isDefective"]
        if pomp["isDefective"] == True:
            errors += 1
    while True:
        response = requests.get(api_url)
        data = json.loads(response.text)
        for pomp in data:
            current_input_value = pomp["isDefective"]
            if current_input_value != previous_input_values[pomp["id"]]:
                if current_input_value == True:
                    errors += 1
                else:
                    errors -= 1
                mqtt_topic = f"werf/error"
                xmessage = str({"name": "old_pump", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[pomp["id"]] = current_input_value

        time.sleep(xsleepx)
        # print("oude pomp errors -> ", errors)


print("MAIN: Started!")
print("---Login first---")
username = input("Gebruiker: ")
password = input("Wachtwoord: ")
os.system('clear')
print("MAIN: Running!")
# Set up the MQTT client (sensors)
client = mqtt.Client()
client.username_pw_set(username, password)
client.connect("localhost", 1883)
client.subscribe("werf/sensoren/#")
client.on_message = checkDiepteSensor
# Set up the MQTT client (stroommetingen)
client2 = mqtt.Client()
client2.username_pw_set(username, password)
client2.connect("localhost", 1883)
client2.subscribe("werf/actoren/+/stroom")
client2.on_message = checkStroomMetingWaterFlow
# Set up the MQTT client (stroommetingen old)
client3 = mqtt.Client()
client3.username_pw_set(username, password)
client3.connect("localhost", 1883)
client3.subscribe("werf/actoren/old/+/stroom")
client3.on_message = checkStroomMetingWaterFlowOld

# Create and start threads
thread1 = threading.Thread(target=client.loop_forever)
thread2 = threading.Thread(target=client2.loop_forever)
thread8 = threading.Thread(target=client3.loop_forever)

thread3 = threading.Thread(target=checkPomp)
thread4 = threading.Thread(target=checkPompOud)
thread5 = threading.Thread(target=checkErrorSensor)
thread6 = threading.Thread(target=checkErrorPump)
thread7 = threading.Thread(target=checkErrorOldPump)

thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()

signal.signal(signal.SIGINT, signal_handler)

