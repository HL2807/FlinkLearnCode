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
 * @Date 2022年03月31日 15:53:10
 * @Version 1.0.0
 * @ClassName Test09_TumblingTimeWindowProcessFunction
 * @Describe ProcessWindowFunction(全窗口函数)
 * 全窗口函数是等到数据都到了再做计算做一次计算。
 *
 * 注意：ProcessWindowFunction不能被高效执行的原因是Flink在执行这个函数之前, 需要在内部缓存这个窗口上所有的元素
 */
public class Test09_TumblingTimeWindowProcessFunction {

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

        //TODO 6、利用全窗口函数Process实现sum操作
        SingleOutputStreamOperator<Integer> result = window.process(
                new ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {

                    //定义一个累加器
                    private Integer count = 0;

                    /**
                     *
                     * @param tuple key
                     * @param context 上下文对象
                     * @param elements 迭代器里面放的是进入窗口的元素
                     * @param out 采集器
                     * @throws Exception
                     */
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>.Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
                        System.out.println("Process......");
                        for (Tuple2<String, Integer> element : elements) {
                            count += element.f1;
                        }

                        out.collect(count);
                    }
                }
        );

        result.print();

        env.execute("TumblingTimeWindowProcessFunction");

    }

}
