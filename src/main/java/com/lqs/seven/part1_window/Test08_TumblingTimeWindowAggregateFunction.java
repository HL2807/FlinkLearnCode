package com.lqs.seven.part1_window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月31日 15:32:46
 * @Version 1.0.0
 * @ClassName Test08_TumblingTimeWindowAggregateFunction
 * @Describe AggregateFunction 增量聚合函数----可以改变数据的类型
 */
public class Test08_TumblingTimeWindowAggregateFunction {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = streamSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(value, 1));
                    }
                }
        );

        //TODO 4、将相同的单词聚合到一起
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy(0);

        /*//TODO 5、开启一个基于时间的滚动窗口，窗口大小设置为5
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5))
        );*/
        //TODO 5、开启一个基于时间的滑动窗口，窗口大小设置为5
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(6), Time.seconds(3))
        );

        //TODO 6、利用窗口函数Aggregate实现sum操作
        SingleOutputStreamOperator<Integer> result = window.aggregate(
                /*
                 * Tuple2<String, Integer>:输入数据类型
                 * Integer：中间计算类型
                 * Integer：输出数据类型
                 * 输入数据类型和输出数据类型相比较是发生了改变的，由Tuple2<String, Integer>数据类型变成了Integer数据类型
                 */
                new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                    /**
                     * 初始化累加器
                     * @return
                     */
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("初始化累加器......");
                        return 0;
                    }

                    /**
                     * 累加操作，相当给累加器赋值
                     * @param value
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                        System.out.println("累加操作......");
                        return accumulator + value.f1;
                    }

                    /**
                     * 获取最终结果
                     * @param accumulator
                     * @return
                     */
                    @Override
                    public Integer getResult(Integer accumulator) {
                        System.out.println("获取计算结果");
                        return accumulator;
                    }

                    /**
                     * 合并累加器，只在会话窗口中，因为会话窗口需要对多个窗口进行合并，那么每个窗口中的计算结果也需要合并，
                     * 所有在这个方法中合并的是累加器，其余窗口不会调用这个方法
                     * @param a
                     * @param b
                     * @return
                     */
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        System.out.println("合并累加器......");
                        return a + b;
                    }
                }
        );

        result.print();

        env.execute("TumblingTimeWindowAggregateFunction");

    }

}
