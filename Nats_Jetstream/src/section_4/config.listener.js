import { connect, Events } from "nats";

// ./infrastructure에 접속해서 docker compose down 하면 disconnect 탐지
// docker-compose down

async function main() {
    try {
        console.log("연결 시작")

        const nc = await connect({
            servers: [
                'localhost:4222', // 우리가 띄운 nats
                'localhost:4223', // 백업서버 -> 저 서버가 없으면 무시
                'localhost:4224'
            ],
            name: 'client-test',

            // 재연결 관련된 설정
            reconnect: true, // false : 연결이 끊어지면, 클라이언트 더이상 동작 X
            maxReconnectAttempts: -1, // -1 : 무한정 재시도
            // -1로 설정 : 모니터링을 도입
            // 10, 20 : 클라이언트 종료
            reconnectTimeWait: 1000, // 재연결 시도하는 상황에서 이 사이의 대기 시간을 밀리초로 지정 : default : 2000ms
            reconnectJitter: 100, // 재시도 지터 : Thundering Herd
            reconnectJitterTLS: 100,

            timeout: 5000,

            pingInterval: 20000, // 서버가 클라이언트에 PING을 보내는 간격
            maxPingOut: 2,

            waitOnFirstConnect: true
        })

        // https://github.com/nats-io/nats.deno/blob/main/nats-base-client/core.ts
        // ConnectionOptions : interface

        setupEventListeners(nc);

        console.log(nc.getServer());

        // 프로그램이 종료되지 않도록 유지
        await new Promise(() => { });

    } catch (error) {
        console.error(' 오류 발생 : ', error.message)
        process.exit(1);
    }
}


function setupEventListeners(nc) {
    // RxJS에서 EventEmitter
    (async () => {
        for await (const status of nc.status()) {
            const now = new Date().toLocaleTimeString();

            switch (status.type) {
                case Events.Disconnect:
                    console.log(`\n [${now}] 서버 연결 끊김`);
                    console.log(` 서버: ${status.data}`);
                    break;

                case Events.Reconnect:
                    console.log(`\n [${now}] 서버 재연결 성공!`);
                    console.log(`   서버: ${status.data}`);
                    break;

                case Events.Update: // 크러스터 정보 업데이트 될 떄 발생
                    console.log(`\n [${now}] 연결 상태 업데이트`);
                    console.log(`   Added: ${status.data.added?.length || 0}`);
                    console.log(`   Deleted: ${status.data.deleted?.length || 0}`);
                    break;

                case Events.LDM: // Lame Duck Mode
                    console.log(`\n [${now}] Lame Duck Mode 감지`);
                    console.log('   서버가 곧 종료됩니다. 다른 서버로 전환합니다.');
                    break;

                case Events.Error:
                    console.error(`\n [${now}] 연결 오류`);
                    console.error(`   오류: ${status.data}`);
                    break;
            }
        }
    })();
}

main()