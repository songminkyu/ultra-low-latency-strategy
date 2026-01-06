package com.example.flink;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class StateManagementExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 체크포인트 활성화 (10초마다)
        env.enableCheckpointing(10000);

        // 사용자 활동 이벤트 스트림 생성
        DataStream<UserActivity> activityStream = env.addSource(new UserActivitySource());

        // 상태 관리를 사용하여 사용자별 통계 계산
        activityStream
                .keyBy(activity -> activity.userId)
                .process(new UserStatisticsProcessor())
                .print();

        env.execute("State Management Example");
    }


    public  static class UserStatisticsProcessor
        extends KeyedProcessFunction<String, UserActivity, String> {

        // ValueState with TTL: 총 활동 수 (1시간 후 만료)
        private transient ValueState<Long> totalActivitiesState;

        // ValueState: 총 금액
        private transient ValueState<Double> totalAmountState;

        // ListState: 최근 활동 타입 (최대 5개)
        private transient ListState<String> recentActivitiesState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();

            // value() 호출 : TTL이 만료 -> Null

            ValueStateDescriptor<Long> totalActivitiesStateDescriptor =
                    new ValueStateDescriptor<>("totalActivities", Long.class);
            totalActivitiesStateDescriptor.enableTimeToLive(ttlConfig);

            totalActivitiesState = getRuntimeContext().getState(totalActivitiesStateDescriptor);

            ValueStateDescriptor<Double> totalAmountDescriptor =
                    new ValueStateDescriptor<>("totalAmount", Double.class);

            totalAmountState = getRuntimeContext().getState(totalAmountDescriptor);

            ListStateDescriptor<String> recentDescriptor
                    = new ListStateDescriptor<>("recentActivities", String.class);

            recentActivitiesState = getRuntimeContext().getListState(recentDescriptor);
        }

        @Override
        public void processElement(
                UserActivity activity,
                Context ctx,
                Collector<String> collector
        ) throws Exception {
            Long totalActivities = totalActivitiesState.value();
            totalActivitiesState.update((totalActivities == null ? 0L : totalActivities) + 1);

            Double totalAmount = totalAmountState.value();
            totalAmountState.update((totalAmount == null ? 0.0 : totalAmount) + activity.amount);

            List<String> recentActivities = new ArrayList<>();
            for (String s : recentActivitiesState.get()) {
                recentActivities.add(s);
            }
            recentActivities.add(activity.activityType);

            if(recentActivities.size() > 5) {
                recentActivities = recentActivities.subList(
                        recentActivities.size() -5,
                        recentActivities.size()
                );
            }

            recentActivitiesState.update(recentActivities);

            String result = String.format(
                    "User: %s | Total: %d | Amount: %.2f | Recent: %s",
                    activity.userId,
                    totalActivitiesState.value(),
                    totalAmountState.value(),
                    recentActivities
            );
            collector.collect(result);
        }
    }

    public static class UserActivitySource implements SourceFunction<UserActivity> {
        private volatile boolean running = true;
        private final Random random = new Random();
        private final String[] users = {"user1", "user2", "user3"};
        private final String[] activityTypes = {"click", "purchase", "view"};

        @Override
        public void run(SourceContext<UserActivity> ctx) throws Exception {
            while (running) {
                String userId = users[random.nextInt(users.length)];
                String activityType = activityTypes[random.nextInt(activityTypes.length)];
                double amount = activityType.equals("purchase") ? random.nextDouble() * 100 : 0.0;

                ctx.collect(new UserActivity(userId, activityType, amount));
                Thread.sleep(500 + random.nextInt(500));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class UserActivity {
        public String userId;
        public String activityType;  // "click", "purchase", "view"
        public double amount;

        public UserActivity() {}

        public UserActivity(String userId, String activityType, double amount) {
            this.userId = userId;
            this.activityType = activityType;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "UserActivity{userId='" + userId + "', type='" + activityType + "', amount=" + amount + '}';
        }
    }
}
