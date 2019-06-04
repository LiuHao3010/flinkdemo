package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text=env.socketTextStream("localhost",9999);
        DataStream<Tuple2<String,Integer>> wordcount=text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word:s.split("\\s")
                     ) {
                    collector.collect(Tuple2.of(word,1));
                }
            }
        });
        DataStream<Tuple2<String,Integer>> windowcount =wordcount
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        windowcount.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}
