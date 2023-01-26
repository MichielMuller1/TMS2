#!/usr/bin/env python3

# TMS2 Raspberry Pi (Server) back-end script.
# Dit script ............

from datetime import datetime
import paho.mqtt.client as mqtt
import json, requests, threading, signal, sys, time, math

#--

def led_range(percentage, total_leds):
    return math.ceil((percentage/100) * total_leds)    

def signal_handler(sig, frame):
    client.disconnect()
    client2.disconnect()
    # client3.disconnect()
    # client4.disconnect()
    # client5.disconnect()
    print('Exited!')
    quit()
    
#--

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

def checkStroomMeting(client, userdata, message):   
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "value": payload["stroomValue"], "pumpId": payload["actorId"]}
    print(payload)
    # Payload verzenden
    headers = {'Content-type': 'application/json'}
    xurl = "https://hooyberghs-api.azurewebsites.net/api/pumpvalue/"
    response = requests.post(xurl, json=data, headers=headers)
    # Error code²
    if response.status_code in (200, 201):
        print("Succesvol: Payload verzenden naar database gelukt. (Stroommeting: " + payload["actorId"] + ")")
    else:
        print("Mislukt: Payload verzenden naar database mislukt! (Stroommeting: " + payload["actorId"] + ")")
               
def checkStroomMetingOld(client, userdata, message):   
    # Aanmaken van de payload
    timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
    payload = json.loads(message.payload.decode())
    data = {"date": timestamp, "value": payload["stroomValue"], "oldPumpId": payload["actorId"]}
    # Payload verzenden
    headers = {'Content-type': 'application/json'}
    xurl = "https://hooyberghs-api.azurewebsites.net/api/oldpumpvalue/"
    response = requests.post(xurl, json=data, headers=headers)
    # Error code
    if response.status_code in (200, 201):
        print("Succesvol: Payload verzenden naar database gelukt. (Stroommeting: " + "old " + payload["actorId"] + ")")
    else:
        print("Mislukt: Payload verzenden naar database mislukt! (Stroommeting: " + "old " + payload["actorId"] + ")")
                    
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
                # print(xmessage)
                client.publish(mqtt_topic, xmessage)
                print(f"Wijziging: {pump['name']} inputValue: {current_input_value}")
                previous_input_values[pump["id"]] = current_input_value
                
        time.sleep(2)
        
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
        time.sleep(2) 

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
                    errors -=1
                mqtt_topic = "werf/error"
                xmessage = str({"name": "sensor", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[sensor["id"]] = current_input_value

                
        time.sleep(3)
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
                    errors -=1  
                mqtt_topic = f"werf/error"
                xmessage = str({"name": "pump", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[pomp["id"]] = current_input_value
                          
        time.sleep(3)
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
                    errors -=1  
                mqtt_topic = f"werf/error"
                xmessage = str({"name": "old_pump", "isDefective": str(current_input_value), "errors": str(errors)})
                # xmessage = current_input_value
                print(xmessage)
                client.publish(mqtt_topic, xmessage)
                previous_input_values[pomp["id"]] = current_input_value

            
        time.sleep(3)
        # print("oude pomp errors -> ", errors)
         
                                  
print("---Script Running---")         
# Set up the MQTT client (sensors)
client = mqtt.Client()
client.connect("localhost", 1883)
client.subscribe("werf/sensoren/#")
client.on_message = checkDiepteSensor
# Set up the MQTT client (stroommetingen)
client2 = mqtt.Client()
client2.connect("localhost", 1883)
client2.subscribe("werf/actoren/+/stroom")
client2.on_message = checkStroomMeting
# Set up the MQTT client (stroommetingen old)
client3 = mqtt.Client()
client3.connect("localhost", 1883)
client3.subscribe("werf/actoren/old/+/stroom")
client3.on_message = checkStroomMetingOld

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

