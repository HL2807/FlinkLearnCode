package com.lqs.five.part1_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 17:30:05
 * @Version 1.0.0
 * @ClassName Test04_SourceKafka
 * @Describe 从Kafka读取数据流
 */
public class Test04_SourceKafka {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从Kafka读取数据流
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("nwh120:9092")
                .setTopics("test_kafka_source")
                .setGroupId("lqs_test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        streamSource.print();

        env.execute("SourceKafka");

    }

}
