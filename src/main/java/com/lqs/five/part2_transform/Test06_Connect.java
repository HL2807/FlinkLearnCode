package com.lqs.five.part2_transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author lqs
 * @Date 2022年03月28日 21:32:20
 * @Version 1.0.0
 * @ClassName Test06_Connect
 * @Describe connect算子可以连接两个保持他们类型的数据流，两个数据流被connect之后，只是被放在了一个同一个流中，
 * 内部依然保持各自的数据和形式不发生任何变化，两个流相互独立。
 */
public class Test06_Connect {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从元素中获取数据，并创建两条流
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d", "e");

        //TODO 3、使用connect连接两条流
        ConnectedStreams<Integer, String> connect = integerDataStreamSource.connect(stringDataStreamSource);

        //貌合神离
        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value*value+"";
            }

            @Override
            public String map2(String value) throws Exception {
                return value+"aaa";
            }
        }).print();

        env.execute("Connect");

    }

}
