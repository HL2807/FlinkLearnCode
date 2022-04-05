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

/**
 * @Author lqs
 * @Date 2022年04月05日 15:14:27
 * @Version 1.0.0
 * @ClassName Test01_KeyedStateValueState
 * @Describe ValueState<T>
 * 保存单个值. 每个key有一个状态值.  设置使用 update(T), 获取使用 T value()
 *
 * 需求：
 *      检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
 */
public class Test01_KeyedStateValueState {

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

        //TODO 5、添加ValueState
        SingleOutputStreamOperator<String> valueStateResult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    //TODO 定义一个状态用来报错上一次水位值
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("value-state", Integer.class)
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //TODO 拿当前水位和上一次的水位做对比，如果大于10则报警
                        //从状态中去出上一次的水位
                        Integer lastVc = valueState.value() == null ? value.getVc() : valueState.value();
                        if (Math.abs(value.getVc() - lastVc) > 10) {
                            out.collect("水位线超过10.......");
                        }

                        //将当前水位更新到状态中
                        valueState.update(value.getVc());
                    }
                }
        );

        valueStateResult.print();

        env.execute("KeyedStateValueState");

    }

}
