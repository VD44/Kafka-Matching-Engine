const {Kafka} = require('kafkajs');
const os = require("os");
run();
async function run(){
  try{
    const kafka = new Kafka({
      "clientId":"exchange",
      "brokers":[`${os.hostname()}:9092`]
    });
    const admin = kafka.admin();
    console.log("Connecting...");
    await admin.connect();
    console.log("Connected!");
    await admin.createTopics({
      "topics":[
        {
          "topic":"MatchIn",
          "numPartitions":1
        },
        {
          "topic":"MatchOut",
          "numPartitions":1
        }
      ]
    });
    console.log("Created!");
    await admin.disconnect();
  }catch(e){
    console.error('Something bad happened ${e}');
  }
  finally{
        process.exit(0);
    }
}
