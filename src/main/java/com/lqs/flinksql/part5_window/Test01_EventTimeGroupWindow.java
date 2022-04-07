package com.lqs.flinksql.part5_window;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * @Author lqs
 * @Date 2022年04月07日 16:41:02
 * @Version 1.0.0
 * @ClassName Test01_EventTimeGroupWindow
 * @Describe 窗口，在Table API中使用窗口
 * 分组窗口（Group Windows）会根据时间或行计数间隔，将行聚合到有限的组（Group）中，并对每个组的数据执行一次聚合函数。
 * Table API中的Group Windows都是使用。Window（w:GroupWindow）子句定义的，并且必须由as子句指定一个别名。
 * 为了按窗口对表进行分组，窗口的别名必须在group by子句中，像常规的分组字段一样引用。
 */
public class Test01_EventTimeGroupWindow {

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
        Table table = streamTableEnvironment.fromDataStream(
                waterSensorSingleOutputStreamOperator,
                $("id"), $("ts"), $("vc"), $("pt")
                        .proctime()
        );
        //为已有的字段指定为事件时间
//        Table table = streamTableEnvironment.fromDataStream(
//                waterSensorSingleOutputStreamOperator,
//                $("id"),$("ts").rowtime(),$("vc")
//        );

        //TODO 5、GroupWindow
        table
                //TODO 开启一个滚动窗口，窗口大小为3s
                //定义滚动窗口并给窗口起一个别名
                .window(Tumble.over(lit(3).seconds()).on($("ts")).as("w"))
                //TODO 开启一个滑动窗口，窗口大小为3s，滑动步长为2s
//                .window(Slide.over(lit(3).seconds()).every(lit(2).second()).on($("ts")).as("w"))
                //TODO 开启一个会话窗口，会话间隔为2s
//                .window(Session.withGap(lit(2).seconds()).on($("ts")).as("w"))
                //TODO 开一一个基于元素个数的滚动窗口，窗口大小为2，必须指定时间字段，并且是处理时间字段
//                .window(Tumble.over(rowInterval(2L)).on($("pt")).as("w"))
                // 窗口必须出现的分组字段中
                .groupBy($("id"), $("w"))
                .select($("id"),$("vc").sum().as("vcSum"),$("w").start().as("start"), $("w").end().as("end"))
//                .select($("id"),
//                        $("vc").sum().as("vcSum"))
                .execute()
                .print();

    }

}
