package com.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Random;

public class EventTimeExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 타임스탬프가 있는 이벤트 스트림 생성
        DataStream<Event> eventStream = env.addSource(new EventSource());

        // Event Time 설정
        DataStream<Event> eventTimeStream = eventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.eventTime)
                );

        // 1. Event Time 기반 윈도우 (10초 윈도우)
        System.out.println("--- Event Time 윈도우 (이벤트 발생 시간 기준) ---");
        eventTimeStream
                .keyBy(event -> "all")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new EventTimeWindowFunction())
                .print();

        // 2. Processing Time 기반 윈도우 (10초 윈도우)
        System.out.println("\n--- Processing Time 윈도우 (처리 시간 기준) ---");
        eventStream
                .keyBy(event -> "all")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessingTimeWindowFunction())
                .print();

        env.execute("Event Time Example");
    }

    public static class ProcessingTimeWindowFunction
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        private static final DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("HH:mm:ss")
                .withZone(ZoneId.systemDefault());

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Event> elements,
                Collector<String> out) {

            long count = 0;
            int sum = 0;
            for (Event event : elements) {
                count++;
                sum += event.value;
            }

            String result = String.format(
                    "[PROCESSING TIME] 윈도우 [%s ~ %s] | 이벤트 수: %d | 합계: %d",
                    formatter.format(Instant.ofEpochMilli(context.window().getStart())),
                    formatter.format(Instant.ofEpochMilli(context.window().getEnd())),
                    count,
                    sum
            );
            out.collect(result);
        }
    }

    public static class EventTimeWindowFunction
            extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        private static final DateTimeFormatter formatter = DateTimeFormatter
                .ofPattern("HH:mm:ss")
                .withZone(ZoneId.systemDefault());

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Event> elements,
                Collector<String> out) {

            long count = 0;
            int sum = 0;
            for (Event event : elements) {
                count++;
                sum += event.value;
            }

            String result = String.format(
                    "[EVENT TIME] 윈도우 [%s ~ %s] | 이벤트 수: %d | 합계: %d",
                    formatter.format(Instant.ofEpochMilli(context.window().getStart())),
                    formatter.format(Instant.ofEpochMilli(context.window().getEnd())),
                    count,
                    sum
            );
            out.collect(result);
        }
    }

    public static class EventSource implements SourceFunction<Event> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private int counter = 0;

        // Event Time을 5초 전으로 설정 (의도적 지연)
        private long eventTimeOffset = -5000;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running && counter < 35) {
                counter++;

                // Event Time: 현재 시간 - 5초
                long eventTime = System.currentTimeMillis() + eventTimeOffset;

                Event event = new Event(counter, eventTime);
                ctx.collect(event);

                if (counter % 5 == 0) {
                    System.out.println(String.format(
                            "[생성 #%d] Event Time과 Processing Time의 차이: %d ms",
                            counter,
                            event.processingTime - event.eventTime
                    ));
                }

                Thread.sleep(800);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class Event {
        public int value;
        public long eventTime;      // 이벤트가 발생한 시간
        public long processingTime; // 처리되는 시간

        public Event() {}

        public Event(int value, long eventTime) {
            this.value = value;
            this.eventTime = eventTime;
            this.processingTime = System.currentTimeMillis();
        }

        @Override
        public String toString() {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
                    .withZone(ZoneId.systemDefault());
            return String.format("Event{value=%d, eventTime=%s, processingTime=%s}",
                    value,
                    formatter.format(Instant.ofEpochMilli(eventTime)),
                    formatter.format(Instant.ofEpochMilli(processingTime))
            );
        }
    }
}
