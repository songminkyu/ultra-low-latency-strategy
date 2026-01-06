/*
    청크 단위로 동작

    100 MB -> 128KB
    1. 메모리 효율성 
    2. 스트리밍 처리
    3. 분산 처리
    4. 메시지 크기 제한

     Bucket : "app-config"
        -> 'profile/alic.jpg' (2MB)
        -> 'profile/test.jpg' (1.5MB)
*/

import { connect, headers } from 'nats';

async function main() {
    const nc = await connect({ servers: 'localhost:4222' });
    const js = nc.jetstream();


    console.log('bucket 생성 \n')

    const os = await js.views.os('my-files-2', {
        description: '파일 저장소'
        // 128KB
    })

    console.log(' 텍스트 파일 저장 \n')

    const textContent = `
NATS Object Store 테스트 파일

이것은 Object Store에 저장되는 텍스트 파일입니다.
내부적으로 청크로 분할되어 JetStream에 저장됩니다.

생성 시간: ${new Date().toISOString()}
`.trim();

    const encoder = new TextEncoder();
    const textData = encoder.encode(textContent);

    const h = headers();
    h.set('Content-Type', 'text/plain');
    h.set('Author', 'Alice');

    const stream = new ReadableStream({
        start(controller) {
            controller.enqueue(textData);
            controller.close();
        }
    });

    await os.put({
        name: "documents/test.txt",
        description: ' 테스트 텍스트 파일',
        headers: h
    }, stream);


    console.log(' 파일 정보 조회 \n')

    const info = await os.info('documents/test.txt');
    if (info.headers) {
        for (const [k, v] of Object.entries(info.headers)) {
            console.log(`${k} : ${v}`)
        }
    }
    console.log()

    console.log('파일 다운로드 \n')

    const result = await os.get('documents/test.txt');

    const chunks = [];
    let chunkCount = 0;

    const reader = result.data.getReader()
    while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        chunks.push(value)
        chunkCount++;
        console.log(` 청크 ${chunkCount} : ${value.length}`)
    }

    const downloaded = Buffer.concat(chunks)
    console.log(`다운로드 완료 : ${downloaded.length}`)

    await nc.drain();
}


main().catch(console.error)