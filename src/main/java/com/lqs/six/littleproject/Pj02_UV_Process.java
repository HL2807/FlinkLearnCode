package com.lqs.six.littleproject;

import com.lqs.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author lqs
 * @Date 2022年03月29日 17:39:50
 * @Version 1.0.0
 * @ClassName Pj02_UV_Process
 * @Describe 使用process方法来实现UV（每日独立访客计算）
 */
public class Pj02_UV_Process {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        streamSource.process(new ProcessFunction<String, Tuple2<String,Integer>>() {

            //第一个个存储器来对数据进行去重和存储
            HashSet<Long> set=new HashSet<Long>();

            @Override
            public void processElement(String value, ProcessFunction<String, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );

                if ("pv".equals(userBehavior.getBehavior())){
                    set.add(userBehavior.getUserId());
                    out.collect(Tuple2.of("uv",set.size()));
                }

            }
        }).print();

        env.execute("Pj02_UV_Process");

    }

}
