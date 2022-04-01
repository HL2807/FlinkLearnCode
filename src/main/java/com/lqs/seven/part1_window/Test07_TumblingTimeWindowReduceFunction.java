package com.lqs.seven.part1_window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
 * @Date 2022年03月31日 14:54:32
 * @Version 1.0.0
 * @ClassName Test07_TumblingTimeWinowReduceFunction
 * @Describe ReduceFunction 增量聚合函数----不会改变数据的类型
 * 增量聚合函数是来一条计算一条
 */
public class Test07_TumblingTimeWindowReduceFunction {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据组成Tuple
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = streamSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value, 1));
                    }
                }
        );

        //TODO 4、将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        //TODO 5、开启一个基于时间的滚动窗口，窗口大小设置我5s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5))
        );

        SingleOutputStreamOperator<String> process = window.process(
                new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>.Context context,
                                        Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        String msg =
                                "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                        + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                }
        );

        //TODO 6、利用窗口函数ReduceFunction实现sum操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.reduce(
                new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        System.out.println("Reduce......");
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }
        );

        result.print();

        env.execute("TumblingTimeWindowReduceFunction");

    }

}
