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
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月01日 21:13:46
 * @Version 1.0.0
 * @ClassName SessionEventTImeWaterMarkMonotonous
 * @Describe Fixed Amount of Lateness(允许固定时间的延迟)
 * 基于时间的会话窗口
 */
public class Test04_SessionEventTimeWaterMarkBounded {

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

        //TODO 4、指定Watermark生成器为Bounded,并设置乱序时间为5秒
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = wordToOneDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        //分配时间戳
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );

        //TODO 5、将形同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDS.keyBy("id");

        //TODO 6、开一一个基于事件时间的窗口
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(
                EventTimeSessionWindows.withGap(Time.seconds(3))
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

        env.execute("SessionEventTimeWaterMarkBounded");

    }

    /*
        1 1 1
        1 6 1
        1 14 1
        第一个延迟输出：当前key: (1)窗口: [1,4) 一共有 1条数据
                     当前key: (1)窗口: [6,9) 一共有 1条数据
                     原因：6（事件时间）+3（会话窗口时间）+5（延迟时间）=14

        1 17 1
        1 22 1
        1 23 1
        1 31 1
        第二个延迟输出：当前key: (1)窗口: [14,20) 一共有 2条数据
                     当前key: (1)窗口: [22,26) 一共有 2条数据
                     当前key: (1)窗口: [31,34) 一共有 1条数据
                     原因：23（事件时间）+3（会话窗口时间）+5（延迟时间）=31
   */

}
