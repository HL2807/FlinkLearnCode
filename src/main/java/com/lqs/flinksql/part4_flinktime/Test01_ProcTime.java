package com.lqs.flinksql.part4_flinktime;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 16:18:18
 * @Version 1.0.0
 * @ClassName Test01_ProcTime
 * @Describe 时间属性
 * 处理时间属性可以在schema定义的时候用.proctime后缀来定义。(处理)时间属性一定不能定义在一个已有字段上，
 * 所以它只能定义在schema定义的最后
 */
public class Test01_ProcTime {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));


        //2.获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 将流转为表 并指定处理时间
        Table table = tableEnv.fromDataStream(
                waterSensorStream, $("id"), $("ts"), $("vc"), $("pt")
                        .proctime()
        );

        table.execute().print();

    }

}
