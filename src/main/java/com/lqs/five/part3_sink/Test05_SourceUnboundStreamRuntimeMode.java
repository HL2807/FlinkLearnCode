package com.lqs.five.part3_sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月29日 16:31:06
 * @Version 1.0.0
 * @ClassName Test05_SourceUnboundStreamRuntimeMode
 * @Describe 执行模式(Execution Mode)
 * 1	STREAMING(默认)：STREAMING模式下, 数据是来一条输出一次结果。
 * 2	BATCH:BATCH模式下, 数据处理完之后, 一次性输出结果。
 * 3	AUTOMATIC:自动模式
 */
public class Test05_SourceUnboundStreamRuntimeMode {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、设置执行模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
//        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 3、获取数据
        //获取无界数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);
        //获取有界数据
//        DataStreamSource<String> streamSource1 = env.readTextFile("input/words.txt");

        //TODO 4、将数据按照空格进行切分，切分出每一个单词
        SingleOutputStreamOperator<String> wordDS = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        });

        //TODO 5、将每个档次组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = wordDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });

        //TODO 6、将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDS.keyBy(value -> value.f0);
//        KeyedStream<Tuple2<String, Integer>, String> keyedStream1 = wordToOneDS.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });

        //7、做累加计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute("SourceUnboundStreamRuntimeMode");

    }

}
