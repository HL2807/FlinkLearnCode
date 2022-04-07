package com.lqs.seven.part4_flinkcep;

import com.lqs.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lqs
 * @Date 2022年04月06日 19:40:16
 * @Version 1.0.0
 * @ClassName Test11_CEPProjectOrderWatch
 * @Describe 订单支付实时监控
 * 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
 */
public class Test11_CEPProjectOrderWatch {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取数据并转换为JavaBean，同时指定Watermark，并按照orderId进行聚合
        KeyedStream<OrderEvent, Tuple> orderIdDS = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new OrderEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<OrderEvent>() {
                                            @Override
                                            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                                                return element.getEventTime() * 1000;
                                            }
                                        }
                                )
                )
                .keyBy("orderId");

        //TODO 3、定义模式
        /**
         * 需求：统计创建订单到下单中间超过15分钟的超时数据以及正常的数据。
         * 统计创建订单（匹配数据）
         * 到下单（匹配数据）
         * 中间超过15分钟（within）
         * 超时塑化剂以及正常的数据（侧输出，分别将两种数据放到不同的流中
         */
        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("pay")
                .where(new IterativeCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value, Context<OrderEvent> ctx) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.seconds(15));

        //TODO 4、将定义的模式应用于流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderIdDS, pattern);

        //TODO 5、获取正常的数据以及超时的数据
        SingleOutputStreamOperator<String> result = patternStream.select(
                new OutputTag<String>("timeout") {
                },
                new PatternTimeoutFunction<OrderEvent, String>() {
                    @Override
                    public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                        return pattern.toString();
                    }
                },
                new PatternSelectFunction<OrderEvent, String>() {
                    @Override
                    public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
                        return pattern.toString();
                    }
                }
        );

        result.print("正常数据>>>>>>");
        result.getSideOutput(new OutputTag<String>("timeout"){}).print("超时数据>>>>>>");

        env.execute();

    }

}
