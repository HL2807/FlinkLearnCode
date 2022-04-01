package com.lqs.five.part2_transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月28日 19:47:36
 * @Version 1.0.0
 * @ClassName Test02_FlatMap
 * @Describe flatMap算子。作用：消费一个元素并产生零个或多个元素
 */
public class Test02_FlatMap {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、通过FlatMap将每一个单词取出来,也可以使用富函数（RichFlatMapFunction）
        streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                //按照空格切分数据并循环写出（单个写出）
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            }
        }).print("FlatMapFunction>>>>>>");

        //简写，使用lambda表达式简写步骤3
        streamSource.flatMap((String value,Collector<String> out)->{
            for (String word : value.split(" ")) {
                out.collect(word);
            }
        }).returns(Types.STRING)//指定返回的类型
                .print();

        //写法2
        streamSource.flatMap(new MyRichFlatMap())
                        .print("RichFlatMapFunction>>>>>>");

        env.execute("FlatMap");

    }

    public static class MyRichFlatMap extends RichFlatMapFunction<String,String>{

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            for (String word : value.split(" ")) {
                out.collect(word);
            }
        }
    }

}
