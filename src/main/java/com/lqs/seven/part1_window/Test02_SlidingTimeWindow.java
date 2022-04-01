package com.lqs.seven.part1_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月30日 17:34:53
 * @Version 1.0.0
 * @ClassName Test02_SlidingTimeWindow
 * @Describe 滑动窗口(Sliding Windows)
 * 与滚动窗口一样, 滑动窗口也是有固定的长度. 另外一个参数我们叫滑动步长, 用来控制滑动窗口启动的频率.
 * 	所以, 如果滑动步长小于窗口长度, 滑动窗口会重叠. 这种情况下, 一个元素可能会被分配到多个窗口中
 * 	例如, 滑动窗口长度10分钟, 滑动步长5分钟, 则, 每5分钟会得到一个包含最近10分钟的数据.
 */
public class Test02_SlidingTimeWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //TODO 4、将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //TODO 5、开启一个基于时间的滑动窗口，窗口大小设置为5s，滑动步长设置为3秒
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                SlidingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(3))
        );

        //显示流程详情
        SingleOutputStreamOperator<String> process = window.process(
                new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        String msg =
                                "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                        + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                }
        );

        //TODO 6、计算计结果
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);

        process.print();

        result.print();

        env.execute("SlidingTimeWindow");

    }

}
