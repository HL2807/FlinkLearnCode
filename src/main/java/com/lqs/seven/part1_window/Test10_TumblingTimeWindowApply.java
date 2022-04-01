package com.lqs.seven.part1_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月31日 16:03:33
 * @Version 1.0.0
 * @ClassName Test10_TumblingTimeWindowApply
 * @Describe  Apply全窗口函数
 */
public class Test10_TumblingTimeWindowApply {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = streamSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }
        );

        //TODO 4、将相同的key进行聚合
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //TODO 5、开启一个基于时间的滚动窗口，开窗大小设置为6s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(6))
        );

        //TODO 6、利用全窗口函数Apply实现sum操作
        SingleOutputStreamOperator<Integer> result = window.apply(
                new WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {

                    private Integer count = 0;

                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
                        System.out.println("Apply......");
                        for (Tuple2<String, Integer> value : input) {
                            count += value.f1;
                        }

                        out.collect(count);
                    }
                }
        );

        result.print();

        env.execute("TumblingTimeWindowApply");

    }

}
