import { Client, Databases, ID} from 'node-appwrite';

import mqtt from "mqtt"
// import sdk, { AppwriteException } from 'node-appwrite';

let client = new Client();
client.setEndpoint(process.env.ENDPOINT_URL)
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
        log("client connect successfully")
        logAppwrite("client connect successfully")
        client_mqtt.subscribe(topicName, (err, granted) => {
            if (err) {
                log(err, 'err');
                logAppwrite("err")
            }
            log(granted, 'granted')
            logAppwrite("granted")
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
                  name: temp.deviceName.split('_')[0],
                  time: temp.rxInfo[1].time,
                  timeTurnOn: "",
                  battery: temp.object.battery,
                  type : temp.deviceProfileName,
                  value: temp.object.temperature ?? 1,
                  status: "on"
                }
              );
              log('Document created successfully');
              logAppwrite("Document created successfully")
            } catch (error) {
              if (error instanceof AppwriteException && error.code === 409) {
                // Document with the requested ID already exists, update the existing document
                await databases.updateDocument(
                  buildingDatabaseID,
                  sensorCollectionID,
                  temp.devEUI,
                  {
                    name: temp.deviceName.split('_')[0],
                    time: temp.rxInfo[1].time,
                    timeTurnOn: "",
                    battery: temp.object.battery,
                    type : temp.deviceProfileName,
                    value: temp.object.temperature ?? 2,
                    status: "on"
                  }
                );
                log('Document updated successfully');
                logAppwrite("Document updated successfully")
              } else {
                throw error;
              }
            }
          } catch (error) {
            error('Error processing message:', error);
          }
    })

    client_mqtt.on("packetsend", (packet) => {
        
    })

    client_mqtt.on("error", function (error) {
        log('err: ', error)
        logAppwrite(`rr: ${error}`)
    })

    client_mqtt.on("close", function () {
        log("closed")
        logAppwrite("closed")
    })

    return res.json({
      message:
        'realtime read senor value',
    });
};

const logCollectionId = "66d18cd100349aec7523";
// Write a function for input is String of Log, and output is a new document in Log Collection
async function logAppwrite(log) {
  try {
    await databases.createDocument(buildingDatabaseID, logCollectionId, ID.unique(), {
      log: log,
      time: new Date().toISOString(),
      type: "MQTT_AppWrite"
    });
  } catch (error) {
    console.error('Error logging:', error);
  }
}