package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 21:54:22
 * @Version 1.0.0
 * @ClassName Test09_Reduce
 * @Describe 一个分组数据流的聚合操作，合并当前的元素和上次聚合的结果，产生一个新的值，返回的流中包含每一次聚合的结果，
 * 而不是只返回最后一次聚合的最终结果。
 */
public class Test09_Reduce {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、使用map将从端口获取的数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、先对相同的id进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //TODO 5、使用聚合算子reduce实现Max功能
        keyedStream.reduce(new ReduceFunction<WaterSensor>() {

            /**
             * 一个分组的第一条数据来的时候，不会进入reduce方法
             * 输入和输出的数据类型一定要一样
             * @param value1 上一次聚合后的结果
             * @param value2 当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                return new WaterSensor(value1.getId(),value1.getTs(),Math.max(value1.getVc(),value2.getVc()));
            }
        }).print();

        env.execute("Reduce");

    }

}
