import { connect, StringCodec, AckPolicy } from 'nats';

async function main() {
    try {
        const nc = await connect({ servers: 'localhost:4222' })

        const js = nc.jetstream();
        const jsm = await nc.jetstreamManager();
        const sc = StringCodec();

        console.log("Push Consumer 생성")

        try {
            await jsm.consumers.add('ORDERS', {
                durable_name: 'order-pusher', // 기본적인 consumer name
                ack_policy: AckPolicy.Explicit, // 애는 나중에 알아볼거에요.

                deliver_subject: 'deliver.orders', // 메시지를 이 Subject로 자동 전달
                deliver_group: 'order-workers' // Queue Group (선택사항)
            })

            /*
                1. ORDERS Stream에 메시지를 저장
                2. Stream이 자동으로 deliver.orders Subject에 메시지를 재발행
                3. 이 Subject를 구독한다면?? 메시지를 받을 수 있는것
            */
        } catch (error) {
            throw error
        }

        console.log('메시지 3개 발행 중...\n');

        for (let i = 1; i <= 3; i++) {
            const order = {
                orderId: `ORD-${Date.now()}-${i}`,
                amount: 10.00 * i,
            };

            await js.publish('orders.created', sc.encode(JSON.stringify(order)));
            console.log(`  메시지 ${i} 발행: ${order.orderId}`);
        }

        /*
            1. ORDERS Stream이 메시지를 저장 (orders.* 패턴에 매칭)
            2. Push Consumer가 감지
            3. deliver.orders로 재발행 
        */

        console.log('\n메시지 발행 완료\n');

        // Push Consumer 구독
        console.log('메시지 수신 대기 중...\n');

        const sub = nc.subscribe('deliver.orders', { queue: 'order-workers' });

        let count = 0;
        for await (const msg of sub) {
            const order = JSON.parse(sc.decode(msg.data));
            console.log(`메시지 수신: ${order.orderId}`);

            // 메시지 처리
            await new Promise(resolve => setTimeout(resolve, 500));

            // Ack 전송
            msg.respond();  // Push Consumer는 respond()로 Ack
            console.log(' Ack 전송\n');

            count++;
            if (count >= 3) {
                break;
            }
        }

        await nc.drain();
        console.log('연결 종료');


    } catch (err) {
        console.log(err)
    }
}

main()