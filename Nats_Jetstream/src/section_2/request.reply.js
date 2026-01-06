const { connect, StringCodec } = require("nats")


async function main() {
    // 1. Nats 서버 연결
    const nc = await connect({ servers: 'localhost:4222' })
    const sc = StringCodec();

    // Service (Responder)
    const sub = nc.subscribe('user.get');
    (async () => {
        for await (const msg of sub) {
            console.log(`Received reqeust: ${sc.decode(msg.data)}`)

            const response = JSON.stringify({ name: "John", email: "john@example.com" })
            msg.respond(sc.encode(response))
        }
    })();

    // Broad Cast 테스트를 위한 추가적인 consumer
    const sub2 = nc.subscribe('user.get');
    (async () => {
        for await (const msg of sub2) {
            console.log(`Received reqeust 22: ${sc.decode(msg.data)}`)

            const response = JSON.stringify({ name: "John", email: "john@example.com" })
            msg.respond(sc.encode(response))
        }
    })();

    // Client (요청자)
    try {
        const response = await nc.request(
            'user.get',
            sc.encode(JSON.stringify({ userId: 1 })),
            { timeout: 2000 }
        )
        console.log(`Received response: ${sc.decode(response.data)}`)
    } catch (err) {
        console.error('Reqeust feild: ', err.message);
    }

    await nc.drain();
}

main()