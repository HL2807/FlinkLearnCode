package com.lqs.seven.part3_programstate;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月05日 16:16:48
 * @Version 1.0.0
 * @ClassName Test04_KeyedStateAggregatingState
 * @Describe AggregatingState<IN, OUT>:
 * 存储单个值. 与ReducingState类似, 都是进行聚合. 不同的是, AggregatingState的聚合的结果和元素类型可以不一样.
 * 需求：计算每个传感器的平均水位
 */
public class Test04_KeyedStateAggregatingState {

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

        //TODO 5、添加AggregatingState
        SingleOutputStreamOperator<String> AggregateStateResult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    //TODO 创建一个AggregatingState计算平均水平
                    private AggregatingState<Integer, Double> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        aggregatingState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("agg=state",
                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                            /**
                                             * 创建一个新的累加器，开始一个新的聚合。
                                             * @return
                                             */
                                            @Override
                                            public Tuple2<Integer, Integer> createAccumulator() {
                                                return Tuple2.of(0, 0);
                                            }

                                            /**
                                             * 将给定的输入值添加到给定的累加器，返回新的累加器值。
                                             * @param value 要添加的值
                                             * @param accumulator 将值添加到的累加器
                                             * @return 具有更新状态的累加器
                                             */
                                            @Override
                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
                                            }

                                            /**
                                             * 从累加器获取聚合结果。
                                             * @param accumulator 聚合的累加器
                                             * @return 最终聚合结果。
                                             */
                                            @Override
                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                                return accumulator.f0 * 1.0 / accumulator.f1;
                                            }

                                            /**
                                             * 合并两个累加器，返回具有合并状态的累加器。
                                             * <p>这个函数可以重用任何给定的累加器作为合并的目标并返回它。假设是给定的累加器在传递给此函数后将不再使用</p>
                                             * @param a 要合并的累加器
                                             * @param b 另一个要合并的累加器
                                             * @return 合并状态的累加器
                                             */
                                            @Override
                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                                            }
                                        },
                                        Types.TUPLE(Types.INT, Types.INT)
                                )
                        );
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        //将数据存入状态中求平均值
                        aggregatingState.add(value.getVc());
                        //取出状态中计算过后的结果
                        Double vcAvg = aggregatingState.get();
                        out.collect(value.getId() + "_" + vcAvg);
                    }
                }
        );


        AggregateStateResult.print();

        env.execute("KeyedStateValueState");

    }


}
