const { connect, StringCodec } = require('nats');

async function main() {
    // 1. Nats 서버 연결
    const nc = await connect({ servers: 'localhost:4222' })
    const sc = StringCodec();

    const sub1 = nc.subscribe('task.process', { queue: 'workers' });
    (async () => {
        for await (const msg of sub1) {
            console.log(`[Worker 1] Received: ${sc.decode(msg.data)}`)
        }
    })();

    const sub2 = nc.subscribe('task.process', { queue: 'workers' });
    (async () => {
        for await (const msg of sub2) {
            console.log(`[Worker 2] Received: ${sc.decode(msg.data)}`)
        }
    })();

    const sub3 = nc.subscribe('task.process', { queue: 'workers' });
    (async () => {
        for await (const msg of sub3) {
            console.log(`[Worker 3] Received: ${sc.decode(msg.data)}`)
        }
    })();

    for (let i = 1; i <= 10; i++) {
        nc.publish('task.process', sc.encode(`Task ${i}`))
    }

    setTimeout(async () => {
        await nc.drain()
    }, 1000);
}

main()