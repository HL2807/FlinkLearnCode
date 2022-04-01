package com.lqs.five.part2_transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 21:38:34
 * @Version 1.0.0
 * @ClassName Test07_Union
 * @Describe 对两个或者两个以上的DataStream进行union操作，产生一个包含所有DataStream元素的新DataStream
 */
public class Test07_Union {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从元素中获取数据，并创建两条流
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> stringDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);

        //TODO 3、使用union连接两条流
        DataStream<Integer> union = integerDataStreamSource.union(stringDataStreamSource);

        union.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value+2;
            }
        }).print();

        //lambda
        //水乳交融
        union.map(value -> value*2).print();

        env.execute("Union");

    }

}
