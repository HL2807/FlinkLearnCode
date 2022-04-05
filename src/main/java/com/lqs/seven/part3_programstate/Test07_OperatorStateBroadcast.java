package com.lqs.seven.part3_programstate;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月05日 17:06:53
 * @Version 1.0.0
 * @ClassName Test07_OperatorStateBroadcast
 * @Describe 广播状态（Broadcast state）
 * 是一种特殊的算子状态. 如果一个算子有多项任务，而它的每项任务状态又都相同，那么这种特殊情况最适合应用广播状态。
 */
public class Test07_OperatorStateBroadcast {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource1 = env.socketTextStream("nwh120", 9999);

        //TODO 3、定义一个状态并广播
        MapStateDescriptor<String, String> mapState = new MapStateDescriptor<>("map", String.class, String.class);

        //TODO 4、广播状态
        BroadcastStream<String> broadcast = streamSource.broadcast(mapState);

        //TODO 5、连接普通流和广播流
        BroadcastConnectedStream<String, String> connect = streamSource1.connect(broadcast);

        //TODO 6、对两条流的数据进行处理
        SingleOutputStreamOperator<String> broadcastStateResult = connect.process(
                new BroadcastProcessFunction<String, String, String>() {

                    @Override
                    public void processElement(String value, BroadcastProcessFunction<String, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //TODO 提取广播状态
                        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);
                        String aswitch = broadcastState.get("switch");
                        if ("1".equals(aswitch)) {
                            out.collect("指定执行逻辑1");
                        } else if ("2".equals(aswitch)) {
                            out.collect("指定执行逻辑2");
                        } else {
                            out.collect("指定支持逻辑3");
                        }

                    }

                    @Override
                    public void processBroadcastElement(String value, BroadcastProcessFunction<String, String, String>.Context ctx, Collector<String> out) throws Exception {

                        //提取状态
                        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapState);

                        //将数据写入广播状态
                        broadcastState.put("switch", value);

                    }
                }
        );

        broadcastStateResult.print();

        env.execute("OperatorStateBroadcast");

    }

}
