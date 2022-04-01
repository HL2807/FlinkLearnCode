package com.lqs.five.part2_transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 20:01:58
 * @Version 1.0.0
 * @ClassName Test03_Filter
 * @Describe filter过滤算子
 * 作用：根据指定的规则将满足条件（true）的数据保留，不满足条件(false)的数据丢弃
 *
 */
public class Test03_Filter {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、使用Filter算子将偶数获取到并乘2
        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return Integer.parseInt(value)%2==0;
            }
        }).map(
                new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value)*2;
                    }
                }
        ).print();

        //简写,使用lambda简写，对第三步骤
        streamSource.filter(singleNum->Integer.parseInt(singleNum)%2==0)
                        .map(doubleNum->Integer.parseInt(doubleNum)*2).print("lambda>>>>>");

        env.execute("Filter");

    }

}
