package com.lqs.flinksql.part5_window;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/**
 * @Author lqs
 * @Date 2022年04月07日 17:04:51
 * @Version 1.0.0
 * @ClassName Test02_EventTimeOverWindow
 * @Describe Over window聚合是标准SQL中已有的（Over子句），可以在查询的SELECT子句中定义。
 * Over window 聚合，会针对每个输入行，计算相邻行范围内的聚合。
 * Table API提供了Over类，来配置Over窗口的属性。可以在事件时间或处理时间，以及指定为时间间隔、或行计数的范围内，
 * 定义Over windows。
 * 无界的over window是使用常量指定的。也就是说，时间间隔要指定UNBOUNDED_RANGE，或者行计数间隔要指定UNBOUNDED_ROW
 * 。而有界的over window是用间隔的大小指定的。
 */
public class Test02_EventTimeOverWindow {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、创建数据并指定Watermark
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = env.fromElements(
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

        //TODO 3、获取表的执行环境
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env);

        //TODO 4、将流转为表，并指定事件时间
//        Table table = streamTableEnvironment.fromDataStream(
//                waterSensorSingleOutputStreamOperator,
//                $("id"), $("ts"), $("vc"), $("pt")
//                        .proctime()
//        );
        //为已有的字段指定为事件时间
        Table table = streamTableEnvironment.fromDataStream(
                waterSensorSingleOutputStreamOperator,
                $("id"),$("ts").rowtime(),$("vc")
        );


        //TODO 5、OverWindow
        table
//                .window(Over.partitionBy($("id")).orderBy($("ts")).as("w"))
                //TODO 往前两行
//                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(2L)).as("w"))
                //TODO 往前两秒
                .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(2).seconds()).as("w"))
                .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")))
                .execute()
                .print();

    }

}
