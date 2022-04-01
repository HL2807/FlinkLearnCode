package com.lqs.seven.part2_watermark;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月01日 22:40:38
 * @Version 1.0.0
 * @ClassName Test09_TimeServices
 * @Describe 定时器 基于处理时间的定时器
 * 基于处理时间或者事件时间处理过一个元素之后, 注册一个定时器, 然后指定的时间执行，定时器只能用于keyedStream中，即keyby之后使用.
 * Context和OnTimerContext所持有的TimerService对象拥有以下方法:
 * currentProcessingTime(): Long 返回当前处理时间
 * currentWatermark(): Long 返回当前watermark的时间戳
 * registerProcessingTimeTimer(timestamp: Long): Unit 会注册当前key的processing time的定时器。
 * 当processing time到达定时时间时，触发timer。
 * registerEventTimeTimer(timestamp: Long): Unit 会注册当前key的event time 定时器。
 * 当水位线大于等于定时器注册的时间时，触发定时器执行回调函数。
 * deleteProcessingTimeTimer(timestamp: Long): Unit 删除之前注册处理时间定时器。如果没有这个时间戳的定时器，则不执行。
 * deleteEventTimeTimer(timestamp: Long): Unit 删除之前注册的事件时间定时器，如果没有此时间戳的定时器，则不执行
 */
public class Test09_TimeServiceProcessingTime {

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

        //TODO 4、将相同的id数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneDS.keyBy("id");

        //TODO 5、注册基于处理时间的定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        System.out.println(ctx.timerService().currentProcessingTime());
                        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 200);
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
                        System.out.println("生成WaterMark");
                        //System.out.println(timestamp/1000);
                        //out.collect("定时时间到了"+ctx.timerService().currentProcessingTime()/1000);
                        ctx.timerService().registerProcessingTimeTimer(timestamp + 200);
                    }
                }
        );

        result.print();

        env.execute("TimeServiceProcessingTime");

    }

}
