package com.lqs.seven.part2_watermark;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月01日 18:05:36
 * @Version 1.0.0
 * @ClassName Test01_EventTImeWaterMarkMononto
 * @Describe WaterMark生成器:
 * Monotonously Increasing Timestamps(时间戳单调增长:其实就是允许的延迟为0)
 * 基于滚动窗口
 */
public class Test01_TumblingEventTImeWaterMarkMonotonous {

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
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 4、指定Watermark生成器为Monotonously
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = wordToOneDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs() * 1000;
                                    }
                                }
                        )
        );

        //TODO 5、将形同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDS.keyBy("id");

        //TODO 6、开一一个基于事件时间的窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(
                TumblingEventTimeWindows.of(Time.seconds(5))
        );

        window.process(
                new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg = "当前key: " + tuple.toString()
                                + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                }
        ).print();

        env.execute("TumblingEventTImeWaterMarkMonotonous");

    }

}
