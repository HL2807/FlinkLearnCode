package com.lqs.five.part1_source;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Author lqs
 * @Date 2022年03月28日 09:38:19
 * @Version 1.0.0
 * @ClassName CustomizeSource
 * @Describe 自定义source
 */
public class Test05_CustomizeSource {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 2、从自定义的数据源获取数据
        env.addSource(new MySource()).print();

        env.execute("CustomizeSource");

    }

    public static class MySource implements ParallelSourceFunction<WaterSensor>{

        private Boolean isRuning=true;

        /**
         * 自定义数据生成
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (isRuning){
                ctx.collect(
                        new WaterSensor(
                                "test_kafka_source"+new Random().nextInt(100),
                                System.currentTimeMillis(),
                                new Random().nextInt(1000)
                        )
                );
                Thread.sleep(1000);
            }
        }

        /**
         * 取消数据生成
         * 一般在run方法中会有个while循环，通过此方法终止while循环
         */
        @Override
        public void cancel() {
            isRuning=false;
        }
    }

}
