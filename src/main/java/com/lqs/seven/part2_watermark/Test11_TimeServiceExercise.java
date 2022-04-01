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
import org.apache.flink.util.OutputTag;

/**
 * @Author lqs
 * @Date 2022年04月01日 23:00:53
 * @Version 1.0.0
 * @ClassName Test11_TImeService
 * @Describe 定时器练习
 * 需求：
 * 监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信
 */
public class Test11_TimeServiceExercise {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> wordToOneDS = streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 4、将相同的key进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneDS.keyBy("id");

        //TODO 5、检查水位传感器的水位值，如果水位值在5秒中之内连续上升，则报警，并将报警信息侧输出
        SingleOutputStreamOperator<String> rerult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {

                    //定义一个变量保存上一次的水位
                    private Integer lastVc = Integer.MIN_VALUE;

                    //定义一个变量保存定时器时间
                    private Long timer = Long.MIN_VALUE;

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断水位是否上升
                        if (value.getVc() > lastVc) {
                            //如果没有注册定时器，则先注册定时器
                            if (timer == Long.MIN_VALUE) {
                                System.out.println("注册定时器：" + ctx.getCurrentKey());
                                timer = ctx.timerService().currentProcessingTime() + 5000;
                                ctx.timerService().registerProcessingTimeTimer(timer);
                            }
                        } else {
                            //水位没有上升
                            //删除之前已经注册过的定时器
                            if (timer != Long.MIN_VALUE) {
                                System.out.println("删除定时器：" + ctx.getCurrentKey());
                                ctx.timerService().deleteProcessingTimeTimer(timer);
                                //为了方便下次水位上升时注册，需要把定时器时间重置
                                timer = Long.MIN_VALUE;
//                                //同时在注册一个定时器来使用
//                                timer=ctx.timerService().currentProcessingTime()+5000;
//                                ctx.timerService().registerProcessingTimeTimer(timer);
                            }
                        }

                        //无论水位线是否上升，都要将本次的水位存入lastVC，以便下一个水位来做对比
                        lastVc = value.getVc();

                        out.collect(value.toString());

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("ot") {
                        }, "警报，水位正在上升......");
                        //一旦报警，则重置定时器时间，以便下一个5秒来的数据能够注册定时器
                        timer = Long.MIN_VALUE;

                        //重置lastVC
                        lastVc = Integer.MIN_VALUE;
                    }
                }
        );

        rerult.print();

        //获取侧输出流中的警告数
        rerult.getSideOutput(new OutputTag<String>("ot") {
        }).print("警告信息，侧输出");

        env.execute("TimeServiceExercise");

    }

}
