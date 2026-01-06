import { connect, Events, RetentionPolicy, StorageType } from "nats";


// http://localhost:8222/jsz?streams=true

// http://localhost:8222/jsz?streams=true&config=true
// -> 더 상세한 정보 보기

async function main() {
    try {
        const nc = await connect({ servers: 'localhost:4222' })
        console.log('연결 성공 \n');

        const jsm = await nc.jetstreamManager();

        const streamInfo = await jsm.streams.add({
            name: "ORDERS_3",

            subjects: ['orders.*'], // orders.created, orders.updated

            storage: StorageType.File,
            // FIle : 디스크에 저장 -> 서버가 재시작되어도 데이터는 그대로 유지
            // Memroy : 메모리에 저장 -> 속도 좋음, 데이터 유실이 발생 가능

            retention: RetentionPolicy.Limits,
            // Limits : 크기/갯수/시간 제한 적용
            // Interest : Consumer가 있을떄만 메시지를 보관
            // Workqueue : 작업 큐 패턴 -> 메시지가 한번 소비가 된다?? 그러면 바로 삭제

            max_age: 7 * 24 * 60 * 60 * 1000000000,
            // 메시지 보관 기간 (7일)

            max_bytes: 1024 * 1024 * 1024,
            // 바이트 단위 -> 1GB

            max_msgs: 100000,
            // Stream에 저장될 수 있는 최대 메시지 갯수

            duplicate_window: 2 * 60 * 1000000000
            // 2분 (나노초) 
        })

        console.log(`이름 : ${streamInfo.config.name}`)
        await nc.drain()
    } catch (err) {
        console.log(err)
        // console.error(err)
    }
}


main()