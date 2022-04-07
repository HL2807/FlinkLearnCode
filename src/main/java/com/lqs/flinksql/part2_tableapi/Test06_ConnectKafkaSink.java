package com.lqs.flinksql.part2_tableapi;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Date 2022年04月07日 11:57:29
 * @Version 1.0.0
 * @ClassName Test06_ConnectKafkaSink
 * @Describe 通过Connector声明写出数据
 * 写出到Kafka
 */
public class Test06_ConnectKafkaSink {

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

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        Table table = tableEnvironment.fromDataStream(waterSensorStream);

        Table select = table.select($("id"), $("ts"), $("vc"));

        //TODO 连接外部到Kafka，写出数据
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnvironment.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"nwh120:9092")
        )
                .withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");

        select.executeInsert("sensor");

    }

}
