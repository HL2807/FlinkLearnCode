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
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月30日 16:47:44
 * @Version 1.0.0
 * @ClassName Test01_TumblingWindows
 * @Describe 流式计算是一种被设计用于处理无限数据集的数据处理引擎，而无限数据集是指一种不断增长的本质上无限的数据集，
 * 而Window窗口是一种切割无限数据为有限块进行处理的手段。
 * 	在Flink中, 窗口(window)是处理无界流的核心. 窗口把流切割成有限大小的多个"存储桶"(bucket), 我们在这些桶上进行计算.
 *
 * 	基于时间的窗口
 *      时间窗口包含一个开始时间戳(包括)和结束时间戳(不包括), 这两个时间戳一起限制了窗口的尺寸.
 * 	    在代码中, Flink使用TimeWindow这个类来表示基于时间的窗口.  这个类提供了key查询开始时间戳和结束时间戳的方法,
 * 	    还提供了针对给定的窗口获取它允许的最大时间差的方法(maxTimestamp())
 *
 * 	    滚动窗口(Tumbling Windows)
 * 	    滚动窗口有固定的大小, 窗口与窗口之间不会重叠也没有缝隙.比如,如果指定一个长度为5分钟的滚动窗口, 当前窗口开始计算,
 * 	    每5分钟启动一个新的窗口.
 * 	    滚动窗口能将数据流切分成不重叠的窗口，每一个事件只能属于一个窗口。
 */
public class Test01_TumblingTimeWindow {

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

        //TODO 5、开启一个基于时间的滚动查看，窗口大小设置为5s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(10))
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

        env.execute("TumblingTimeWindow");


    }

}
