package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class WordCountExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.addSource(new SentenceSource());

        DataStream<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .sum(1);

        wordCounts.print();

        env.execute("Word Count Example");
    }

    public static class SentenceSource implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private final String[] sentences = {
                "Apache Flink is a stream processing framework",
                "Flink provides exactly once semantics",
                "Stream processing is different from batch processing",
                "Flink supports event time and processing time",
                "Stateful stream processing with Flink"
        };

        private final Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (isRunning) {
                ctx.collect(sentences[random.nextInt(sentences.length)]);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static final  class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.toLowerCase().split("\\W+");
            for (String word : words) {
                if (word.length() > 0) { // (단어, 1) 쌍
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
