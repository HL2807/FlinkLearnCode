package com.lqs.seven.part1_window;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年03月30日 17:56:47
 * @Version 1.0.0
 * @ClassName Test03_SessionWithDynameicGapTimeWindow
 * @Describe 话窗口(Session Windows)动态gap
 */
public class Test04_SessionWithDynameicGapTimeWindow {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换成Tuple2元组
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、将相同的单词聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //TODO 5、开启一个基于时间的滑动窗口，使用动态间隔
        WindowedStream<WaterSensor, Tuple, TimeWindow> window = keyedStream.window(
                ProcessingTimeSessionWindows.withDynamicGap(
                        new SessionWindowTimeGapExtractor<WaterSensor>() {
                            @Override
                            public long extract(WaterSensor element) {
                                return element.getTs();
                            }
                        }
                )
        );

        //显示流程详情
        SingleOutputStreamOperator<String> process = window.process(
                new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        String msg =
                                "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd() / 1000 + ") 一共有 "
                                        + elements.spliterator().estimateSize() + "条数据 ";
                        out.collect(msg);
                    }
                }
        );

        process.print();

        env.execute("SessionWithDynameicGapTimeWindow");

    }

}
