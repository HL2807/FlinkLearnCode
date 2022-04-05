package com.lqs.seven.part3_programstate;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author lqs
 * @Date 2022年04月05日 15:29:31
 * @Version 1.0.0
 * @ClassName Test02_KeyedStateListState
 * @Describe ListState<T>:
 * 保存元素列表.
 * 添加元素: add(T)  addAll(List<T>)
 * 获取元素: Iterable<T> get()
 * 覆盖所有元素: update(List<T>)
 *
 * 需求： 针对每个传感器输出最高的3个水位值
 */
public class Test02_KeyedStateListState {

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

        //TODO 5、添加ListState
        SingleOutputStreamOperator<String> listStateResult = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, String>() {

                    //TODO 创建ListState用来存放三个最高的水位值
                    private ListState<Integer> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //初始化状态
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-state", Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        /*//1、将当前水位报错到状态中
                        listState.add(value.getVc());
                        //2、将状态中的数据取出来
                        Iterable<Integer> vcIter = listState.get();
                        //3、创建一个list集合用来存放迭代器中的数据
                        ArrayList<Integer> vcList = new ArrayList<>();
                        for (Integer integer : vcIter) {
                            vcList.add(integer);
                        }
                        //4、对集合中的数据由小到大进行排序
                        vcList.sort(
                                new Comparator<Integer>() {
                                    @Override
                                    public int compare(Integer o1, Integer o2) {
                                        return o2 - o1;
                                    }
                                }
                        );
                        //5、判断集合中的元素个数是否大于三，大于三的话删除最后一个（最小的数据）
                        if (vcList.size() > 3) {
                            vcList.remove(3);
                        }
                        //6、将list集合中的数据更新至状态中
                        listState.update(vcList);

                        out.collect(vcIter.toString());*/

                        //先将状态中的数据取出来
                        Iterable<Integer> vcIter = listState.get();
                        ArrayList<Integer> vcList = new ArrayList<>();
                        for (Integer integer : vcIter) {
                            vcList.add(integer);
                        }

                        vcList.add(value.getVc());

                        vcList.sort(
                                new Comparator<Integer>() {
                                    @Override
                                    public int compare(Integer o1, Integer o2) {
                                        return o2 - o1;
                                    }
                                }
                        );

                        //5、判断集合中的元素个数是否大于三，大于三的话删除最后一个（最小的数据）
                        if (vcList.size() > 3) {
                            vcList.remove(3);
                        }
                        //6、将list集合中的数据更新至状态中
                        listState.update(vcList);

                        out.collect(vcIter.toString());
                    }
                }
        );

        listStateResult.print();

        env.execute("KeyedStateListState");

    }


}
