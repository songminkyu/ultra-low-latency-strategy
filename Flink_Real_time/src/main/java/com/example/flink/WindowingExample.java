package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Random;

public class WindowingExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<ClickEvent> clickStream = env
                .addSource(new ClickEventSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<ClickEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        // 1. Tumbling Window
        clickStream
                .keyBy(event -> event.userId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new ClickCountAggregator())
                .print();

        // 2. Sliding Window
        clickStream
                .keyBy(event -> event.userId)
                .window(SlidingEventTimeWindows.of(
                        Time.seconds(30),
                        Time.seconds(10)
                ))
                .aggregate(new ClickCountAggregator())
                .print();

        // 12:00:00 ~ 12:00:30
        // 12:00:10 ~ 12:00:40

        env.execute("Windowing Example");
    }

    public static class ClickCountAggregator
            implements AggregateFunction<ClickEvent, Tuple2<String, Long>, Tuple2<String, Long>> {

        @Override
        public Tuple2<String, Long> createAccumulator() {
            return new Tuple2<>("0", 0L);
        }

        @Override
        public Tuple2<String, Long> add(ClickEvent value, Tuple2<String, Long> accumulator) {
            return new Tuple2<>(value.userId, accumulator.f1 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }

    public static class ClickEventSource implements SourceFunction<ClickEvent> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"user1", "user2", "user3"};
        private final String[] pages = {"/home", "/product", "/cart"};

        @Override
        public void run(SourceContext<ClickEvent> ctx) throws Exception {
            while (running) {
                String userId = users[random.nextInt(users.length)];
                String page = pages[random.nextInt(pages.length)];

                ctx.collect(new ClickEvent(userId, page, System.currentTimeMillis()));
                Thread.sleep(200 + random.nextInt(300));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static class ClickEvent {
        public String userId;
        public String page;
        public long timestamp;

        public ClickEvent(String userId, String page, long timestamp) {
            this.userId = userId;
            this.page = page;
            this.timestamp = timestamp;
        }

        public ClickEvent() {}

        @Override
        public String toString() {
            return "ClickEvent{userId='" + userId + "', page='" + page + "'}";
        }
    }
}
