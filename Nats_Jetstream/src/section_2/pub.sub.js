const { connect, StringCodec } = require("nats")


async function main() {
    // 1. Nats 서버 연결
    const nc = await connect({ servers: 'localhost:4222' })
    const sc = StringCodec();

    const sub = nc.subscribe('orders.created');
    (async () => {
        for await (const msg of sub) {
            console.log(`Received: ${sc.decode(msg.data)}`)
        }
    })();

    nc.publish('orders.created', sc.encode(JSON.stringify({ orderId: '12345' }))) // Core Nats fire-and-forget

    setTimeout(async () => {
        await nc.drain()
    }, 1000);
}

main()