/*
    애플리케이션 설정 값 (config)
    피처 플래그 ( feature flags )
    캐시 데이터 
    세션 정보

    --> 작은 크기의 데이터
*/

/*
    1. Bucket
    -> 쌍들의 컨테이너 데이터베이스로 생각하면 테이블

    Bucket : "app-config"
        -> db.host = 'localhost'
        -> db.port = 5432
        -> cache.ttl = 3600

    2. Revision ( 버전 관리 )
    -> AWS S3 ( Object Storage가 이런 버전 관리)

    theme = "light" (Revision 1)
    them = "dark" (Revision 2)

    3. History
    4. TTL
*/

import { connect } from 'nats';

async function main() {
    const nc = await connect({ servers: 'localhost:4222' });
    const js = nc.jetstream();

    console.log('버킷 생성 \n');

    const kv = await js.views.kv('app-config-2', {
        history: 5
    })

    console.log(' 값을 저장 \n')

    await kv.put('db.host', 'localhost')
    await kv.put('cache.ttl', '3600') // Revision 2를 가져가버림

    console.log(' 값을 조회 \n')

    const dbHost = await kv.get('db.host')
    console.log(`db.host = ${dbHost.string()}`)
    console.log(`db.host revision = ${dbHost.revision}`) // Optimistic Locking

    console.log(' update 진행 \n')

    await kv.put('db.host', 'test')
    // --> db.host => Revision : 2

    const dbHost2 = await kv.get('db.host')
    console.log(`db.host2 = ${dbHost2.string()}`)
    console.log(`db.host2 revision = ${dbHost2.revision}`)


    console.log(' 모든 키 조회 \n')

    const keys = await kv.keys(); // `db.*` 을 통해서 db.으로 시작하는 모든 값 필터링도 가능

    for await (const key of keys) {
        const entry = await kv.get(key)
        console.log(` ${key} = ${entry.string()}`)
    }

    // console.log(' 키를 삭제 \n')
    // await kv.delete('원하는 키 삭제')

    console.log(' 실시간 변경 감지 WATCH \n')

    const testWatcher = await kv.watch({ key: 'theme' });

    (async () => {
        for await (const entry of testWatcher) {
            if (entry.operation === "PUT") {

            } else if (entry.operation === "DEL") {

            }
        }
    })();


    await nc.drain();
}

main()

