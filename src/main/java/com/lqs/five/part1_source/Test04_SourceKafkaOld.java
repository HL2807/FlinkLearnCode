package com.lqs.five.part1_source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author lqs
 * @Date 2022年03月28日 17:30:05
 * @Version 1.0.0
 * @ClassName Test04_SourceKafka
 * @Describe 从Kafka读取数据流
 */
public class Test04_SourceKafkaOld {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从Kafka读取数据流
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "nwh120:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "lqs_test");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        env.addSource(new FlinkKafkaConsumer<String>("test_kafka_source", new SimpleStringSchema(), properties)).print();

        env.execute("SourceKafkaOld");

    }

}
