package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 20:31:34
 * @Version 1.0.0
 * @ClassName Test05_Shuffle
 * @Describe shuffle算子
 * 作用；
 * 把流中的元素随机打乱. 对同一个组数据, 每次只需得到的结果都不同.
 */
public class Test05_Shuffle {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、使用map将从端口读进来的字符串转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).setParallelism(2);

        //TODO 4、使用shuffle将id随机分到不同分区中
        DataStream<WaterSensor> shuffle = map.shuffle();

        map.print("原始数据>>>>>>").setParallelism(2);
        shuffle.print("shuffle");

        env.execute("Shuffle");

    }

}
