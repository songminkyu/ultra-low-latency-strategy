import { connect, StringCodec, AckPolicy } from 'nats';

async function main() {
    try {
        const nc = await connect({ servers: 'localhost:4222' })

        const js = nc.jetstream();
        const jsm = await nc.jetstreamManager();
        const sc = StringCodec();

        console.log("Pull Consumer 생성")

        try {
            await jsm.consumers.add('ORDERS', {
                durable_name: 'order-puller',

                ack_policy: AckPolicy.Explicit, // 명시적으로 확인한다.

                ack_wait: 30 * 1000000000, // 30초

                max_deliver: 3, // 최대 재전송 횟수

                deliver_policy: 'all' //  Push Consumer
            })

            console.log("Pull Consumer 생성")
        } catch (err) {
            throw err
        }

        for (let i = 1; i <= 5; i++) {
            const order = {
                orderId: `ORD-${Date.now()}-${i}`,
                userId: `USER-${100 + i}`,
                amount: 10.00 * i,
                items: i,
                timestamp: new Date().toISOString()
            };

            await js.publish('orders.created', sc.encode(JSON.stringify(order)));
            console.log(`  메시지 ${i} 발행: ${order.orderId}`);
        }

        console.log('Pull Consumer로 메시지 가져오기 시작\n');

        const consumer = await js.consumers.get('ORDERS', 'order-puller');

        const batch1 = await consumer.fetch({ max_messages: 2 });

        for await (const msg of batch1) {
            const order = JSON.parse(sc.decode(msg.data));
            console.log(`  메시지 수신: ${order.orderId}`);

            // 메시지 처리 시뮬레이션
            await new Promise(resolve => setTimeout(resolve, 500));

            // Ack 전송
            msg.ack();
            console.log(`Ack 전송\n`);
        }

        console.log('2초 대기 (백프레셔 시뮬레이션)...\n');
        await new Promise(resolve => setTimeout(resolve, 2000));

        console.log('배치 2: 메시지 3개 요청...\n');

        const batch2 = await consumer.fetch({ max_messages: 3 });

        for await (const msg of batch2) {
            const order = JSON.parse(sc.decode(msg.data));
            console.log(`  메시지 수신: ${order.orderId}`);

            // 메시지 처리 시뮬레이션
            await new Promise(resolve => setTimeout(resolve, 500));

            // Ack 전송
            msg.ack();
            console.log(`     Ack 전송\n`);
        }

        console.log('모든 메시지 처리 완료\n');
        // 연결 종료
        await nc.drain();


    } catch (err) {
        console.log(err)
    }
}

main()