package com.lqs.flinksql.part2_tableapi;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 09:31:50
 * @Version 1.0.0
 * @ClassName Demo
 * @Describe 基本使用，表与DataStream的混合使用
 */
public class Test01_Demo {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //TODO 3、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 4、将流转换为表
        Table t = tableEnvironment.fromDataStream(waterSensorDataStreamSource);

        //TODO 5、通过连续查询获取数据
        Table resultTable = t.where($("id").isEqual("sensor_1"))
                .select($("id"), $("ts"), $("vc"));

        //TODO 6、将表转换为流
        DataStream<Row> result = tableEnvironment.toAppendStream(resultTable, Row.class);

        result.print();

        env.execute();

    }

}
