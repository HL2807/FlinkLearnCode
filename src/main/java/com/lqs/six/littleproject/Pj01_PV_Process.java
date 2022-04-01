package com.lqs.six.littleproject;

import com.lqs.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月29日 17:17:50
 * @Version 1.0.0
 * @ClassName Pj01_PV_Method2
 * @Describe 计算页面PV（page view）总浏览量，用process
 */
public class Pj01_PV_Process {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //TODO 3、使用process计算pv
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {

            //累加器初始化
            private Integer count = 0;

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

                if ("pv".equals(userBehavior.getBehavior())) {
                    count++;
                    out.collect(Tuple2.of("pv", count));
                }
            }

        });

        result.print();

        env.execute("Pj01_PV_Process");

    }

}
