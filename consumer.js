const {Kafka} = require('kafkajs');
const os = require("os");
run();
async function run(){
  try{
    const kafka = new Kafka({
      "clientId":"exchange",
      "brokers":[`${os.hostname()}:9092`]
    });
    const consumer = kafka.consumer({"groupId":"test"});
    console.log("Connecting...");
    await consumer.connect();
    console.log("Connected!");
    await consumer.subscribe({
      "topic":"MatchOut",
      "fromBeginning":true
    });
    await consumer.run({
      "eachMessage":async result => {console.log(`${result.message.key} ${result.message.value}`);}
    })
    console.log("Created!");
  }catch(e){
    console.error(`Something bad happened ${e}`);
  }
}
