package com.lqs.seven.part2_watermark;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
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

import java.time.Duration;

/**
 * @Author lqs
 * @Date 2022年04月01日 21:33:47
 * @Version 1.0.0
 * @ClassName Test05_EventTimeCustomizeWaterMarkGenerator
 * @Describe periodic(周期性) and punctuated(间歇性).
 * 都需要继承接口: WatermarkGenerator
 * 包含间歇性生成和周期性生成WaterMark
 */
public class Test05_TumblingEventTimeCustomizeWaterMarkGenerator {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置WaterMark中的生成周期时间为1s，其默认生成时间是200ms
        env.getConfig().setAutoWatermarkInterval(1000);

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

        //TODO 4、指定Watermark生成器为自定义生成,并设置乱序时间为5秒
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = wordToOneDS.assignTimestampsAndWatermarks(
                new WatermarkStrategy<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new MyWaterMarkGenerator(Duration.ofSeconds(3));
                    }
                }
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

        env.execute("TumblingEventTimeCustomizeWaterMarkGenerator");

    }

    //自定义一个类实现WatermarkGenerator接口
    public static class MyWaterMarkGenerator implements WatermarkGenerator<WaterSensor> {


        /**
         * 到目前为止遇到的最大时间戳。
         */
        private long maxTimestamp;

        /**
         * 此水印生成器假定的最大无序性。
         */
        private long outOfOrdernessMillis;

        public MyWaterMarkGenerator(Duration maxOutOfOrderness) {
            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // 开始，这样我们的最低水印将是 Long.MIN_VALUE。
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        /**
         * 间歇性生成WaterMark
         *
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            System.out.println("间歇性生成WaterMark："+(maxTimestamp-outOfOrdernessMillis-1));
            output.emitWatermark(new Watermark(maxTimestamp-outOfOrdernessMillis-1));
        }

        /**
         * 周期性生成WaterMark
         *
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
//            System.out.println("周期性生成(间隔1秒)WaterMark：" + (maxTimestamp - outOfOrdernessMillis - 1));
//            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }
    }

}
