package com.lqs.five.part1_source;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年05月08日 21:08:48
 * @Version 1.0.0
 * @ClassName Test06_SourceSocket
 * @Describe 从Socket读取数据，也是无界流数据的读取
 */
public class Test03_SourceSocket {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、读取无界流数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、转换数据结构
        //TODO 写法1
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = streamSource.flatMap(
                new RichFlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        //按空格切分
                        String[] split = value.split(" ");
                        //循环将切分好的数据发送到下游（下一级）
                        for (String word : split) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                }
        );

        //TODO 写法2
        /*SingleOutputStreamOperator<String> lineToWord = streamSource.flatMap(
                new RichFlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        String[] words = value.split(" ");
                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                }
        );
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineToWord.map(
                new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        return Tuple2.of(value, 1L);
                    }
                }
        );*/

        //TODO 写法3
        /*SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = streamSource.flatMap(
                        (String line, Collector<String> out) -> {
                            Stream<String> words = Arrays.stream(line.split(" "));
//                    words.forEach(word -> out.collect(word));
                            words.forEach(out::collect);
                        }
                ).returns(Types.STRING)
                .map(value -> Tuple2.of(value, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));*/

        //TODO 4、分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOne.keyBy(
                new KeySelector<Tuple2<String, Long>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Long> value) throws Exception {
                        return value.f0;
                    }
                }
        );
        //简写
//        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOne.keyBy(value -> value.f0);

        //TODO 5、求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        //TODO 6、打印
        result.print();

        //TODO 7、执行
        env.execute("WordCount");

    }

}
