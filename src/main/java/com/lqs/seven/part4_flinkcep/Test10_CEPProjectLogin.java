package com.lqs.seven.part4_flinkcep;

import com.lqs.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author lqs
 * @Date 2022年04月06日 17:55:41
 * @Version 1.0.0
 * @ClassName Test10_CEPProjectLogin
 * @Describe 恶意监控登录
 * 用户2秒内连续两次及以上登录失败则判定为恶意登录。
 */
public class Test10_CEPProjectLogin {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据并转为JavaBean，并指定Watermark,同时按照userId相同的值聚合到一起
        KeyedStream<LoginEvent, Tuple> loginDS = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(
                                Long.parseLong(split[0]),
                                split[1],
                                split[2],
                                Long.parseLong(split[3])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<LoginEvent>() {
                                            @Override
                                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                                return element.getEventTime() * 1000;
                                            }
                                        }
                                )
                )
                .keyBy("userId");
        //TODO 3、定义模式（CEP）
        /**
         *
         * 用户2秒内连续两次及以上登录失败则判定为恶意登录。
         * 连续：循环模式的连续性，严格连续
         * 两次及以上（循环两次及两次以上）
         * 登录失败（通过条件判断事件类型为Fail
         * 满足上面的条件则判定为恶意登录
         */
        Pattern<LoginEvent, LoginEvent> pattern = Pattern
                .<LoginEvent>begin("fail")
                .where(new IterativeCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .timesOrMore(2)
                //TODO 严格连续
                //指定任何不匹配的元素都会中断循环。
                .consecutive()
                .within(Time.seconds(2));

        //TODO 4、将模式作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginDS, pattern);

        //TODO 5、获取符合规则的数据
        patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.get("fail").toString();
            }
        }).print();

        env.execute();

    }

}
