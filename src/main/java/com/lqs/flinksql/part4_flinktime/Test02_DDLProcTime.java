package com.lqs.flinksql.part4_flinktime;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 16:22:07
 * @Version 1.0.0
 * @ClassName Test02_DDLProcTime
 * @Describe 在创建表的 DDL 中定义
 */
public class Test02_DDLProcTime {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 3、在创建表语句中指定处理时间
        tableEnvironment.executeSql(
                "create table sensor (id string,ts bigint,vc int,pt as PROCTIME()) with(" +
                        "'connector' = 'filesystem'," +
                        "'path' = 'input/sensor-sql.txt'," +
                        "'format' = 'csv'" +
                        ")"
        );

        tableEnvironment.executeSql("select * from sensor").print();

    }

}
