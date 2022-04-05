package com.lqs.seven.part3_programstate;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
 * @Date 2022年04月05日 16:42:40
 * @Version 1.0.0
 * @ClassName Test05_KeyedStateMapState
 * @Describe MapState<UK, UV>:
 * 存储键值对列表.
 * 添加键值对:  put(UK, UV) or putAll(Map<UK, UV>)
 * 根据key获取值: get(UK)
 * 获取所有: entries(), keys() and values()
 * 检测是否为空: isEmpty()
 *
 * 需求：去重: 去掉重复的水位值. 思路: 把水位值作为MapState的key来实现去重, value随意
 */
public class Test05_KeyedStateMapState {

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

        //TODO 5、添加 mapState
        SingleOutputStreamOperator<String> mapStateResult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {

                    //TODO 创建MapState状态
                    private MapState<Integer, WaterSensor> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //TODO 初始化状态
                        mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, WaterSensor>(
                                        "map-state",
                                        Integer.class,
                                        WaterSensor.class
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //判断key是否存在
                        if (!mapState.contains(value.getVc())) {
                            //不在的话将水位存入map状态中
                            mapState.put(value.getVc(), value);
                        }

                        out.collect(mapState.get(value.getVc()).toString());
                    }
                }
        );


        mapStateResult.print();

        env.execute("KeyedStateValueState");

    }


}
