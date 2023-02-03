const mqtt = require("mqtt")
const axios = require("axios")

const host = 'broker.emqx.io'
const port = '1883'
const username = "emqx"
const password = "public"
const protocol = "mqtt"

const TOPIC = "pi2/sylph/measurements_test"

const BACKEND_URL = "http://localhost:5050/api/totems"

const METRICS = [
    { name: 'smoke', generator: Math.random() * 10 },
    { name: 'co2', generator: Math.random() * 2 }, 
    { name: 'temperature', generator: Math.random() * 40 }, 
    { name: 'pressure', generator: Math.random() * 100000 }, 
    { name: 'altitude', generator: Math.random() * 1500 }, 
    { name: 'co', generator: Math.random() }, 
    { name: 'no2', generator: Math.random() }, 
    { name: 'nh3', generator: Math.random() }, 
]

const client = mqtt.connect({
    host,
    port,
    protocol,
    username,
    password,
})

const connect = () => {
    return new Promise((resolve, reject) => {
        client.on('connect', () => {
            console.log('Connected')
            client.subscribe([TOPIC], (e) => { 
                if(e) reject()
                console.log(`Subscribe to topic '${TOPIC}'`)
                resolve()
            })
        })
    })
}

const generateMetrics = async () => {
    let count = 0

    const totems = await fetchTotems()
    if(!totems) return

    console.log("totems available", totems)

    while(true){
        const key = String(Math.random())
        for(let metric of METRICS){
            const measurement = {
                [metric.name]: metric.generator, 
                datetime: new Date().toISOString(), 
                key, 
                totem_id: totems[count].id
            }
            await sleep(1000)
            await publish(measurement)
        }
        count = (count + 1) % totems.length
    }
}

const publish = (measurement) => new Promise((resolve, reject) => {
    client.publish(TOPIC, JSON.stringify(measurement), { qos: 0, retain: false }, (error) => {
        if (error) {
          reject(error)
        }
        console.log("published")
        resolve()
    })
})

const fetchTotems = async () => {
    const response = await axios.get(BACKEND_URL)
    return response.data
}

const sleep = (time) => new Promise(resolve => setTimeout(resolve, time))

connect().then(generateMetrics)