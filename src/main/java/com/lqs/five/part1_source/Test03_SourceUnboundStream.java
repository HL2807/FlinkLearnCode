package com.lqs.five.part1_source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月28日 17:02:10
 * @Version 1.0.0
 * @ClassName Test03_SourceUnboundStream
 * @Describe 读取无界数据流
 */
public class Test03_SourceUnboundStream {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取无界数据流
        //使用的nacat进行数据模拟
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据按照空格切分，切出每个单词
        SingleOutputStreamOperator<String> wordDataStream = streamSource.flatMap(
                new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String s, Collector<String> out) throws Exception {
                        for (String s1 : s.split(" ")) {
                            out.collect(s1);
                        }
                    }
                }
        )
                //设置共享度
//                .slotSharingGroup("gourp1")
                //与前后都断开
//                .disableChaining()
                //与前面都断开任务链
//                .startNewChain()
                //为某个算子单独设置并行度
//                .setParallelism(5)
                ;

        //TODO 4、将每个单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDataStream = wordDataStream.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                }
        );

        //TODO 5、将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedDataStream = wordToOneDataStream.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }
        );

        //TODO 1、做累加计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDataStream.sum(1);

        result.print();

        env.execute("SourceUnboundStream");

    }

}
