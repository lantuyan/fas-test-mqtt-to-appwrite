import { Client, Databases } from 'node-appwrite';

import mqtt from "mqtt"
// import sdk, { AppwriteException } from 'node-appwrite';

let client = new Client();
client.setEndpoint("https://cloud.appwrite.io/v1")
    .setProject(process.env.APPWRITE_FUNCTION_PROJECT_ID)
    .setKey(process.env.APPWRITE_API_KEY)

const databases = new Databases(client);
const buildingDatabaseID = process.env.BUILDING_DATABASE_ID;
const sensorCollectionID = process.env.SENSOR_COLLECTION_ID;
const mqtt_url =  process.env.MQTT_URL;
const mqtt_applicationID = process.env.MQTT_APPLICATION_ID;

export default async ({ req, res, log, error }) => {

    var client_mqtt = mqtt.connect(mqtt_url)
    let topicName = `application/${mqtt_applicationID}/device/+/event/up`

    client_mqtt.on("connect", function () {
        console.log("client connect successfully")
        client_mqtt.subscribe(topicName, (err, granted) => {
            if (err) {
                console.log(err, 'err');
            }
            console.log(granted, 'granted')
        })
    })

    client_mqtt.on('message', async (topic, message, packet) => {
        try {
            const temp = JSON.parse(message);

            try {
              await databases.createDocument(
                buildingDatabaseID,
                sensorCollectionID,
                temp.devEUI,
                {
                  deviceName: temp.deviceName,
                  devEUI: temp.devEUI,
                  Battery: temp.object.Battery,
                  Smoke: temp.object.Smoke,
                  Type: temp.object.Type,
                  time: temp.rxInfo[0].time,
                }
              );
              console.log('Document created successfully');
            } catch (error) {
              if (error instanceof AppwriteException && error.code === 409) {
                // Document with the requested ID already exists, update the existing document
                await databases.updateDocument(
                  buildingDatabaseID,
                  sensorCollectionID,
                  temp.devEUI,
                  {
                    deviceName: temp.deviceName,
                    devEUI: temp.devEUI,
                    Battery: temp.object.Battery,
                    Smoke: temp.object.Smoke,
                    Type: temp.object.Type,
                    time: temp.rxInfo[0].time,
                  }
                );
                console.log('Document updated successfully');
              } else {
                throw error;
              }
            }
          } catch (error) {
            console.error('Error processing message:', error);
          }
    })

    client_mqtt.on("packetsend", (packet) => {
        
    })

    client_mqtt.on("error", function (error) {
        console.log('err: ', error)
    })

    client_mqtt.on("close", function () {
        console.log("closed")
    })
};