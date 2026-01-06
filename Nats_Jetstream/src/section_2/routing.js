/*
    orders.new => 2개의 토큰
    orders.us.new 
    users.profile.updated

    Orders.new != orders.new
*/


/*
    orders.us.new, orders.kr.new

    root : orders
        us      kr
       new      new
*/

/*
    Single Token WildCard

    events.*.*.critical -> 4개의 토큰
    -> events.us.server01.critical
    -> events.eu.database.critical

    매칭 X
    -> events.us.critical : 토큰이 3개
    -> events.us.server01.warning.critical : 토큰 5개
    -> a.us.server01.critical : prefixl 토큰이 다르잖아
*/

/*
    Multi Token WildCard

    orders.>
    -> orders.new
    -> orders.us.new
    -> orders.us.east.new

    매칭 X
    -> order.new : orders가 아님
*/


const { connect, StringCodec } = require("nats")

async function main() {
    // 1. Nats 서버 연결
    const nc = await connect({ servers: 'localhost:4222' })
    const sc = StringCodec();

    const sub = nc.subscribe('orders.*.new');
    (async () => {
        for await (const msg of sub) {
            console.log(`Received reqeust: ${msg.subject} : ${sc.decode(msg.data)}`)
        }
    })();

    nc.publish('orders.kr.new', sc.encode('한국 신규 주문'))
    nc.publish('orders.eu.new', sc.encode('유럽 신규 주문'))
    nc.publish('orders.us.new', sc.encode('미국 신규 주문'))


    setTimeout(async () => {
        await nc.drain()
    }, 1000);
}

main()