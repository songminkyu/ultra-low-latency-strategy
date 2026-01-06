package com.example.flink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class CheckpointExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        // 5초마다 체크 포인트를 생성
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 각 레코드를 정확히 한 번만 처리함을 보장
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 체크포인트 간 최소 가격
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 체크포인트가 시간 내에 완료가 되어야한다.
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 동시에 진행할 수 있는 체크포인트 갯수

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3,
                        Time.seconds(10)
                )
        );

        DataStream<Integer> numberStream = env.addSource(new NumberSource());

        // 상태를 사용하여 누적 카운트 계산
        numberStream
                .keyBy(value -> "global")
                .process(new StatefulCounter())
                .print();

        env.execute("Checkpoint Example");
    }


    public static class StatefulCounter extends
            KeyedProcessFunction<String, Integer, String> {

        private transient ValueState<Long> totalCountState;
        private transient ValueState<Boolean> hasFailedState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> countDescriptor =
                    new ValueStateDescriptor<>("totalCount", Long.class);
            totalCountState = getRuntimeContext().getState(countDescriptor);

            ValueStateDescriptor<Boolean> failedDescriptor =
                    new ValueStateDescriptor<>("hasFailed", Boolean.class);
            hasFailedState = getRuntimeContext().getState(failedDescriptor);
        }

        @Override
        public void processElement(
                Integer value, Context ctx, Collector<String> out
        ) throws Exception {
            Long totalCount = totalCountState.value();
            if (totalCount == null) {
                totalCount = 0L;
            }

            totalCount += 1;
            totalCountState.update(totalCount);

            String result = String.format(
                    "[입력: %d] 누적 카운트: %d | 체크포인트 저장됨",
                    value,
                    totalCount);
            out.collect(result);

            Boolean failed = hasFailedState.value();
            if (totalCount == 20 & (failed == null || !failed)) {
                hasFailedState.update(true);
                System.out.println("\n!!! 장애 발생: 카운트 20 도달 !!!");
                System.out.println("!!! 마지막 체크포인트에서 자동 복구 시작... !!!\n");
                throw new RuntimeException("인위적 장애 발생 (카운트 20)");
            }

            if (totalCount >= 30) {
                System.out.println("\n=== 성공적으로 완료! ===");
            }
        }
    }

    public static class NumberSource implements SourceFunction<Integer> {
        private volatile boolean running = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                counter++;
                ctx.collect(counter);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
