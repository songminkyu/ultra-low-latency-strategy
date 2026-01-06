/*
    1. Acks : 확인 및 응답 -> Consumer가 메시지를 받아서 처리한 후, 내가 처리 했다고 알려준다.
    -> 이 메시지를 어떤 방식으로 처리했다.
*/

import { connect } from 'nats';

async function acks() {
    const nc = await connect({ servers: 'localhost:4222' });
    const js = nc.jetstream();

    // Consumer에서 메시지를 받는다고 가정
    const consumer = await js.consumers.get('ORDERS', 'some-consumer');
    const messages = consumer.consume();

    for await (const msg of messages) {
        msg.acks() // 메시지 처리 완료, 제거 -> 재전송을 하지 않아야 하기 떄문에

        msg.nak() // 부정 응답 -> 즉시 재전송을 요청 ( max_deliver )

        msg.nak(5000) // nak(delay) -> 5초 후 재전송

        msg.working(); // ack_wait : 30초 -> 30초안에 어떠한 Acks도 보내지 않는다?? 그럼 실패로 간주

        msg.term(); // Terminate : 그냥 메시지 버려주세요.

        // Acks 없음 ( 타임 아웃 )
        // acks_wait, 자동 재전송 
    }
}