package com.lqs.seven.part2_watermark;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月01日 22:48:26
 * @Version 1.0.0
 * @ClassName Test10_TIme
 * @Describe 基于处理时间的定时器
 */
public class Test10_TimeServiceEventTime {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转为JavaBean类型
        SingleOutputStreamOperator<WaterSensor> wordToOneDS = streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]) * 1000, Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 4、设置WaterMark
        SingleOutputStreamOperator<WaterSensor> waterMarkDS = wordToOneDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 5、将相同的id数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = waterMarkDS.keyBy("id");

        //TODO 6、注册基于事件时间的定时器
        SingleOutputStreamOperator<String> process = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //注册一个5秒的定时器
                        System.out.println("当前时间：" + ctx.timestamp());
                        System.out.println("WaterMark时间：" + ctx.timerService().currentWatermark());
                        System.out.println("注册一个定时器，将在：" + (ctx.timestamp() + 5000 / 1000) + "时触发");
                        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 5000);
                        out.collect(value.toString());
                    }

                    /**
                     * 回调方法，这个方法要用来处理定时器到达定时时间后的逻辑
                     * @param timestamp
                     * @param ctx
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println(timestamp / 1000);
                        out.collect("定时时间到了" + ctx.timestamp() / 1000);
                    }
                }
        );

        process.print();

        env.execute("TimeServiceEventTime");

    }

}
