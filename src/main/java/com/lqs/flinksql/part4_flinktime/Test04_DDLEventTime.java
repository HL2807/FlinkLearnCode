package com.lqs.flinksql.part4_flinktime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 16:34:10
 * @Version 1.0.0
 * @ClassName Test04_DDLEventTime
 * @Describe 在创建表的 DDL 中定义
 * 事件时间属性可以用 WATERMARK 语句在 CREATE TABLE DDL 中进行定义。WATERMARK
 * 语句在一个已有字段上定义一个 watermark 生成表达式，同时标记这个已有字段为时间属性字段。
 */
public class Test04_DDLEventTime {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 更改时区
        Configuration configuration = tableEnvironment.getConfig().getConfiguration();
        configuration.setString("table.local-time-zone","GMT");

        //TODO 3、在建表语句中指定事件时间
        tableEnvironment.executeSql(
                "create table sensor (" +
                        "id string," +
                        "ts bigint," +
                        "vc int," +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        "watermark for t as t - interval '5' second" +
                        ") with(" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'input/sensor-sql.txt'," +
                        "'format' = 'csv'" +
                        ")"
        );

        tableEnvironment.executeSql("select * from sensor").print();

    }

}
