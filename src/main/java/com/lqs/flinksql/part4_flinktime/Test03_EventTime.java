package com.lqs.flinksql.part4_flinktime;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 16:26:15
 * @Version 1.0.0
 * @ClassName Test03_EventTime
 * @Describe 事件时间：DataStream到Table转换时定义
 * 事件时间允许程序按照数据中包含的时间来处理，这样可以在有乱序或者晚到的数据的情况下产生一致的处理结果。它可以保证从外部存储读取
 * 数据后产生可以复现（replayable）的结果。
 * 除此之外，事件时间可以让程序在流式和批式作业中使用同样的语法。在流式程序中的事件时间属性，在批式程序中就是一个正常的时间字段。
 * 为了能够处理乱序的事件，并且区分正常到达和晚到的事件，Flink 需要从事件中获取事件时间并且产生 watermark（watermarks）。
 */
public class Test03_EventTime {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、创建数据并设置水印
        SingleOutputStreamOperator<WaterSensor> watersensorStream = env.fromElements(
                        new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WaterSensor>() {
                                            @Override
                                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );

        // TODO 3、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 4、将流转换为表，并指定时间
//        Table table = tableEnvironment.fromDataStream(
//                watersensorStream,
//                $("id"), $("ts"), $("vc"), $("et")
//                        .rowtime()
//        );

        //TODO 为已经有的字段指定为事件时间
        Table table = tableEnvironment.fromDataStream(
                watersensorStream,
                $("id"), $("ts").rowtime() , $("vc")
        );

        table.execute().print();

    }

}
