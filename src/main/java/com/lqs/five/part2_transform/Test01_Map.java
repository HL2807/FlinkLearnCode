package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 19:21:59
 * @Version 1.0.0
 * @ClassName Test01_Map
 * @Describe map算子
 * 作用：
 * 将数据流中的数据进行转换, 形成新的数据流，消费一个元素并产出一个元素
 */
public class Test01_Map {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);
        //从文件获取数据
        DataStreamSource<String> streamSource1 = env.readTextFile("input");

        //TODO 3、使用map将从端口读进来的字符串转为JavaBean
        streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
                    }
                }
        ).print("MapFunction>>>>>>");

        //TODO 使用富函数的方式 RichMapFunction
        streamSource.map(new MyRichMap()).print("RichMapFunction>>>>>>");

        env.execute("Map");

    }

    public static class MyRichMap extends RichMapFunction<String,WaterSensor>{

        /**
         * 最先被调用，每个并行度调用一次
         * 声名周期方法
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("开始运行......");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            //获取运行时的上下文章
            System.out.println(getRuntimeContext().getTaskName());

            String[] split = value.split(" ");
            return new WaterSensor(split[0],Long.parseLong(split[1]),Integer.parseInt(split[2]));
        }

        /**
         * 最后被调用，每个并行度调用一次
         * 特殊情况除外：在读文件时，每个并行度调用2此
         * 声名周期方法
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("结束运行......");
        }
    }

}
