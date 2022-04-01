package com.lqs.five.part3_sink;

import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @Author lqs
 * @Date 2022年03月28日 22:28:30
 * @Version 1.0.0
 * @ClassName Test01_KafkaSink
 * @Describe Kafka Sink，将数据缓存（下沉）到Kafka
 * Sink有下沉的意思，在Flink中所谓的Sink其实可以表示为将数据存储起来的意思，也可以将范围扩大，
 * 表示将处理完的数据发送到指定的存储系统的输出操作.
 */
public class Test01_KafkaSink {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换为JSON格式
        SingleOutputStreamOperator<String> StringToJsonDS = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(" ");
                return JSONObject.toJSONString(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        StringToJsonDS.print("kafkaSink>>>>>>");

        //TODO 4、将数据写入Kafka
        StringToJsonDS.addSink(new FlinkKafkaProducer<String>(
                "nwh120:9092",
                "topic_test_kafkaSource",
                new SimpleStringSchema()
                ));

        env.execute("KafkaSink");

    }

}
