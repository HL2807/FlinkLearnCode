package com.lqs.seven.part3_programstate;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lqs
 * @Date 2022年04月05日 16:49:40
 * @Version 1.0.0
 * @ClassName Test06_ExerciseWithKeyedState
 * @Describe 练习 改造需求
 * 监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
 */
public class Test06_ExerciseWithKeyedState {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> wordToOneDS = streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 4、把相同的id聚合起来
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneDS.keyBy("id");

        //TODO 5、监控水位传感器的水位值，如果水位值在五秒钟之内连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<String> valueStateEc = keyedStream.process(
                //参考 com.lqs.seven.part2_watermark.Test11_TimeServiceExercise
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {

                    //定义一个变量保存上一次的水位
                    private ValueState<Integer> lastVc;

                    //定义一个变量保存定时器时间
                    private ValueState<Long> timer;

                    //初始化状态


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class, Integer.MIN_VALUE));

                        timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断水位是否上升
                        if (value.getVc() > lastVc.value()) {
                            if (timer.value() == null) {
                                System.out.println("注册定时器：" + ctx.getCurrentKey());
                                timer.update(ctx.timerService().currentProcessingTime());
                                ctx.timerService().registerProcessingTimeTimer(timer.value());
                            }
                        } else {
                            //水位没有上升
                            if (timer.value() != null) {
                                System.out.println("删除定时器：" + ctx.getCurrentKey());
                                ctx.timerService().deleteProcessingTimeTimer(timer.value());
                                timer.clear();
                            }
                        }

                        //无论水位是否上升，都要将本次的水位存入lastVc，以便下一个水位来做对比
                        lastVc.update(value.getVc());
                        out.collect(value.toString());
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        ctx.output(new OutputTag<String>("output") {
                        }, "警报，水位已连续5秒上升......");
                        //一旦报警，则重置定时器时间，以便下一个5秒来的数据能够注册定时器
                        //清空状态
                        timer.clear();
                        //重置lastVc
                        lastVc.clear();
                    }
                }
        );

        valueStateEc.print();

        env.execute("ExerciseWithKeyedState");

    }

}
