package com.lqs.five.part3_sink;

import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author lqs
 * @Date 2022年03月29日 11:14:01
 * @Version 1.0.0
 * @ClassName Test04_RedisSink
 * @Describe 将数据写入Redis
 */
public class Test02_RedisSink {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换为JSON格式
        SingleOutputStreamOperator<WaterSensor> StringToJsonDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、将数据写入Redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("nwh120")
                .setPort(6379)
                .build();

        StringToJsonDS.print("Redis>>>>>>");

        StringToJsonDS.addSink(new RedisSink<>(jedisPoolConfig, new RedisMapper<WaterSensor>() {

            /**
             * 指定写入Redis的写入命令
             * additionalKey是在使用hash或者sort Set的时候需要指定的，hash类型指的是Redis的大key
             * @return
             */
            @Override
            public RedisCommandDescription getCommandDescription() {
                return new RedisCommandDescription(RedisCommand.HSET,"redisSink_test");
            }

            /**
             * 指定Redis的key
             * @param data
             * @return
             */
            @Override
            public String getKeyFromData(WaterSensor data) {
                return data.getId();
            }

            /**
             * 指定存入Redis的value
             * @param data
             * @return
             */
            @Override
            public String getValueFromData(WaterSensor data) {
                return JSONObject.toJSONString(data);
            }
    }));

        env.execute("RedisSink");

    }

}
