package com.lqs.flinksql.part2_tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 11:41:51
 * @Version 1.0.0
 * @ClassName Test04_ConnectorKafkaSource
 * @Describe 通过Connector声明读入数据,读取Kafka数据
 */
public class Test04_ConnectorKafkaSource {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3、连接外部文件系统，获取数据
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property(ConsumerConfig.GROUP_ID_CONFIG,"Test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"nwh120:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        Table table = tableEnv.from("sensor");

        Table selectTable = table.groupBy($("id"))
                .select($("id"), $("vc").sum());

        //方式1，将表转成流打印
//        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(selectTable, Row.class);
//        dataStream.print();

        //方式2：直接用TableResult对象打印
        selectTable.execute().print();

        //如果没有调用流中的算子，可以不用执行
//        env.execute();

    }

}
