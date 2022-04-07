package com.lqs.flinksql.part3_flinksql;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 15:36:25
 * @Version 1.0.0
 * @ClassName Test01_Demo
 * @Describe 基本使用
 * 查询未注册的表
 */
public class Test01_Demo {

    public static void main(String[] args) {

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

        //TODO 4、将流转换为表（未注册的表）
        Table table = tableEnvironment.fromDataStream(waterSensorDataStreamSource);

        //TODO 5、使用flinkSq查询未注册的表
//        Table sqlQuery = tableEnvironment.sqlQuery("select * from " + table + " where id = 'sensor_1'");
//
//        sqlQuery.execute().print();

//        tableEnvironment.executeSql("select * from " + table + " where id = 'sensor_1'").print();

        //TODO 查询已经注册的表，通过table对象注册一张表
        // 通过流注册一张表
        tableEnvironment.createTemporaryView("sensor",waterSensorDataStreamSource);
        tableEnvironment.executeSql("select * from sensor where id = 'sensor_1'").print();

    }

}
