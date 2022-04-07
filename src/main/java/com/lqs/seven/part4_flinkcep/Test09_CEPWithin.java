package com.lqs.seven.part4_flinkcep;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lqs
 * @Date 2022年04月06日 17:52:39
 * @Version 1.0.0
 * @ClassName Test09_CEPWithin
 * @Describe 超时数据
 * 当一个模式上通过within加上窗口长度后，部分匹配的事件序列就可能因为超过窗口长度而被丢弃。
 */
public class Test09_CEPWithin {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据并转为JavaBean，同时指定WaterMark
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.readTextFile("input/sensor2.txt")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WaterSensor>() {
                                            @Override
                                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );

        //TODO 3、定义规则
        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .begin(
                        Pattern
                                .<WaterSensor>begin("start")
                                .where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor value, IterativeCondition.Context<WaterSensor> ctx) throws Exception {
                                        return "sensor_1".equals(value.getId());
                                    }
                                })
                                .next("end")
                                .where(new IterativeCondition<WaterSensor>() {
                                    @Override
                                    public boolean filter(WaterSensor value, IterativeCondition.Context<WaterSensor> ctx) throws Exception {
                                        return "sensor_2".equals(value.getId());
                                    }
                                })
                )
                .within(Time.seconds(3));

        //TODO 4、将规则作用于流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(waterSensorDS, pattern);

        //TODO 5、获取符合规则的数据
        patternStream.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();

        env.execute();

    }

}
