package com.lqs.five.part1_source;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年03月28日 16:42:20
 * @Version 1.0.0
 * @ClassName SourceList
 * @Describe 从集合里面读取数据
 */
public class Test01_SourceList {

    public static void main(String[] args) throws Exception {
        
        //TODO 1、获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2、从集合中获取数据

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("1", 134L, 34),
                new WaterSensor("2", 1434L, 55),
                new WaterSensor("4", 1367L, 354)
        );

        env.fromCollection(waterSensors).print();

        /*//创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        DataStreamSource<Integer> streamSource = env.fromCollection(list).setParallelism(2);

        //多并行度方法
        env.fromCollection();*/

        //从元素中获取数据
        //The parallelism of non parallel operator must be 1
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5).setParallelism(1);

        streamSource.print();

        env.execute("SourceList");

    }
    
}
