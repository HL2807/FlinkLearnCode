package com.lqs.flinksql.part5_window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 18:55:38
 * @Version 1.0.0
 * @ClassName Test04_SqlOverWindow
 * @Describe Over Window
 */
public class Test04_SqlOverWindow {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 3、创建表
        tableEnvironment.executeSql(
                "create table sensor(" +
                        "id string," +
                        //TODO 注意，在flinkSQL里面没有Long，只有bigint
                        "ts bigint," +
                        "vc int," +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        "watermark for t as t - interval '5' second)" +
                        "with(" +
                        "'connector'='filesystem'," +
                        "'path'='input/sensor-sql.txt'," +
                        "'format'='csv'" +
                        ")"
        );

        System.out.println("===================OverWindow第一种写法======================");
        //TODO 4、开启一个OverWindow
        //方式1
        tableEnvironment.executeSql(
                "select " +
                        "id," +
                        "ts," +
                        "vc," +
                        "sum(vc) over(partition by id order by t)" +
                        " from sensor"
        ).print();

        System.out.println("===================OverWindow第二种写法======================");
        //方式2
        tableEnvironment.executeSql(
                "select " +
                        "id," +
                        "ts," +
                        "vc," +
                        "sum(vc) over w," +
                        "count(vc) over w" +
                        " from sensor" +
                        " window w as (partition by id order by t)"
        ).print();


    }

}
