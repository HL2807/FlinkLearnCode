package com.lqs.seven.part3_programstate;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月05日 16:08:19
 * @Version 1.0.0
 * @ClassName Test03_KeyedStateReducingState
 * @Describe ReducingState<T>:
 * 存储单个值, 表示把所有元素的聚合结果添加到状态中.  与ListState类似, 但是当使用add(T)的时候ReducingState会使用指定的
 * ReduceFunction进行聚合.
 *
 * 需求： 计算每个传感器的水位和
 */
public class Test03_KeyedStateReducingState {

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

        //TODO 5、添加reducingState
        SingleOutputStreamOperator<String> reducingStateResult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {

                    //TODO 创建一个ReducingState用来计算水位和
                    private ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        reducingState = getRuntimeContext().getReducingState(
                                new ReducingStateDescriptor<Integer>("reducing-state",
                                        new ReduceFunction<Integer>() {
                                            @Override
                                            public Integer reduce(Integer value1, Integer value2) throws Exception {
                                                return value1 + value2;
                                            }
                                        },
                                        Integer.class
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //将数据存入状态做累加计算
                        reducingState.add(value.getVc());
                        //取出状态中的塑化剂（累加过后的结果》
                        Integer sumVc = reducingState.get();
                        out.collect(value.getId() + "_" + sumVc);
                    }
                }
        );


        reducingStateResult.print();

        env.execute("KeyedStateReducingState");

    }


}
