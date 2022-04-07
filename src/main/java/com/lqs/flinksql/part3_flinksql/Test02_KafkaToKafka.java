package com.lqs.flinksql.part3_flinksql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author lqs
 * @Date 2022年04月07日 15:59:52
 * @Version 1.0.0
 * @ClassName Test02_KafkaToKafka
 * @Describe 使用sql从Kafka读数据, 并写入到Kafka中
 */
public class Test02_KafkaToKafka {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 3、创建一张表消费kafka数据
        tableEnvironment.executeSql("create table source_sensor(id string,ts bigint,vc int) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_source_sensor'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'properties.group.id' = '1027'," +
                "'scan.startup.mode' = 'latest-offset'," +
                "'format' = 'csv'" +
                ")");

        //TODO 4.创建一张表将数据写到kafka中
        tableEnvironment.executeSql("create table sink_sensor(id string,ts bigint,vc int) with(" +
                "'connector' = 'kafka'," +
                "'topic' = 'topic_sink_sensor'," +
                "'properties.bootstrap.servers' = 'hadoop102:9092'," +
                "'format' = 'csv'" +
                ")");

        //TODO 5.将查询source_sensor这个表的数据插入到sink_sensor中，以此来实现将kafka的一个topic的数据写入另一个topic
        tableEnvironment.executeSql("insert into sink_sensor select * from source_sensor where id = 'sensor_1'");

    }

}
