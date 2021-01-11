const {Kafka} = require('kafkajs');
const os = require("os");
run();
async function run(){
  try{
    const kafka = new Kafka({
      "clientId":"exchange",
      "brokers":[`${os.hostname()}:9092`]
    });
    const producer = kafka.producer();
    console.log("Connecting...");
    await producer.connect();
    console.log("Connected!");
    const partition = 0;

    var get_msg = function (v,p) { return {"topic":"MatchIn","messages":[{"value":v,"partition":p}]} };

    var numAccounts = 10;
    var numSymbols = 3;
    var rake = 3;
    var orders = {};

    for(var i = 0; i < numAccounts; i++){
        var result = await producer.send(get_msg(createAccount(i),0));
        console.log(`Sent successfully! ${JSON.stringify(result)}`);
        result = await producer.send(get_msg(createTransfer(i, randomNormalParam(500*100,250*100)),0));
        console.log(`Sent successfully! ${JSON.stringify(result)}`);
    }
    for(var i = 0; i < (numSymbols/2+1) ; i++){
        var result = await producer.send(get_msg(createSymbol(i),0));
        console.log(`Sent successfully! ${JSON.stringify(result)}`);
    }
    for(var i = 0; i < 100000; i++){
        var result = await producer.send(get_msg(genEvent(numAccounts, numSymbols, rake, orders),0));
        console.log(`Sent successfully! ${JSON.stringify(result)}`);
    }

    console.log("Created!");
    await producer.disconnect();
  }catch(e){
    console.error(`Something bad happened ${e}`);
  }
  finally{
      process.exit(0);
  }
}

function randomNormal() {
    var u = 0, v = 0;
    while(u === 0) u = Math.random();
    while(v === 0) v = Math.random();
    return Math.sqrt( -2.0 * Math.log( u ) ) * Math.cos( 2.0 * Math.PI * v );
}

function randomUniform(range) {
    return Math.floor(Math.random()*range);
}

function randomNormalParam(mean, stdDev) {
    return Math.floor(randomNormal()*stdDev+mean);
}

function createOrder(action, oid, aid, sid, price, size) {
    return JSON.stringify({"action":action,"oid":oid,"aid":aid,
            "sid":sid,"price":price,"size":size});
}

function createAccount(aid) {
    return createOrder(100, 0, aid, 0, 0, 0);
}

function createSymbol(sid) {
    return createOrder(0, 0, 0, sid, 0, 0);
}

function createPayout(sid, success, rake) {
    if(rake > 100) return;
    return createOrder(4, 0, 0, sid * (success ? 1 : -1), 0, 100-rake);
}

function createTransfer(aid, amount) {
    return createOrder(101, 0, aid, 0, 0, amount);
}

function createBuy(aid, sid, price, size, orders) {
    var oid = Math.floor(Math.random()*Number.MAX_SAFE_INTEGER);
    orders[oid] = aid;
    return createOrder(2, oid, aid, sid, price, size);
}

function createSell(aid, sid, price, size, orders) {
    var oid = Math.floor(Math.random()*Number.MAX_SAFE_INTEGER);
    orders[oid] = aid;
    return createOrder(3, oid, aid, sid, price, size);
}

function createCancel(orders) {
    var keys = Object.keys(orders);
    var key = keys[Math.floor(Math.random()*keys.length)];
    if(key == null) return createOrder(4, 0, 0, 0, 0, 0);
    var order = createOrder(4, key, orders[key], 0, 0, 0);
    delete orders[key];
    return order;
}

function genEvent(numAccounts, numSymbols, rake, orders) {
    var e = randomUniform(1000);
    if(e == 0) return createSymbol(randomUniform(numSymbols));
    else if(e == 1) return createPayout(randomUniform(numSymbols),randomUniform(2)==0,rake);
    else if(e == 2 || e == 3) return createTransfer(randomUniform(numAccounts),
        randomNormalParam(0,125*100));
    else if(e > 3 && e <= 335) return createBuy(randomUniform(numAccounts),randomUniform(numSymbols),
        randomNormalParam(50,10),randomNormalParam(50,10),orders);
    else if(e > 335 && e <= 667) return createSell(randomUniform(numAccounts),randomUniform(numSymbols),
        randomNormalParam(50,10),randomNormalParam(50,10),orders);
    else return createCancel(orders);
}
