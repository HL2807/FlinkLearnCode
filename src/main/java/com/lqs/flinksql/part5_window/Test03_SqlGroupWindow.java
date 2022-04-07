package com.lqs.flinksql.part5_window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 18:34:55
 * @Version 1.0.0
 * @ClassName Test01_SqlGroupWindow
 * @Describe SQL查询的分组窗口是通过GROUP BY子句定义的。类似于使用常规GROUP BY语句的查询，窗口分组语句的GROUP BY子句中
 * 带有一个窗口函数为每个分组计算出一个结果。
 */
public class Test03_SqlGroupWindow {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 3、创建表
        tableEnvironment.executeSql(
                "create table sensor(" +
                        "id string," +
                        "ts bigint," +
                        "vc int, " +
                        "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                        "watermark for t as t - interval '5' second)" +
                        "with("
                        + "'connector' = 'filesystem',"
                        + "'path' = 'input/sensor-sql.txt',"
                        + "'format' = 'csv'"
                        + ")"
        );

        System.out.println("===================滚动窗口======================");
        //TODO 4、开启一个滚动窗口
        tableEnvironment.executeSql(
                "select " +
                        "id," +
                        "sum(vc) sumVc," +
                        "tumble_start(t,interval '2' second) as stat," +
                        "tumble_end(t,interval '2' second) as ed" +
                        " from sensor" +
                        " group by id,tumble(t,interval '2' second)"
        ).print();

        System.out.println("===================滑动窗口======================");
        //TODO 开启一个滑动窗口
        tableEnvironment.executeSql(
                "select " +
                        "id," +
                        "sum(vc) sumVc," +
                        "hop_start(t,interval '2' second,interval '3' second) as stat," +
                        "hop_end(t,interval '2' second,interval '3' second) as ed" +
                        " from sensor" +
                        " group by id,hop(t,interval '2' second,interval '3' second)"
        ).print();

        System.out.println("===================会话窗口======================");
        //开启一个会话窗口
        tableEnvironment.executeSql(
                "select " +
                        "id," +
                        "session_start(t,interval '2' second) as stat," +
                        "session_end(t,interval '2' second) as ed" +
                        " from sensor" +
                        " group by id,session(t,interval '2' second)"
        ).print();

    }

}
