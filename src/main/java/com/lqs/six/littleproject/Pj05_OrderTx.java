package com.lqs.six.littleproject;

import com.lqs.bean.OrderEvent;
import com.lqs.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author lqs
 * @Date 2022年03月29日 18:24:44
 * @Version 1.0.0
 * @ClassName Pj05_OrderTx
 * @Describe 订单支付实时监控
 * 对于订单而言，为了正确控制业务流程，也为了增加用户的支付意愿，网站一般会设置一个支付失效时间，超过一段时间不支付的订单就会被取消。
 * 另外，对于订单的支付，我们还应保证用户支付的正确性，这可以通过第三方支付平台的交易数据来做一个实时对账。
 * 需求: 来自两条流的订单交易匹配
 * 对于订单支付事件，用户支付完成其实并不算完，我们还得确认平台账户上是否到账了。而往往这会来自不同的日志信息，
 * 所以我们要同时读入两条流的数据来做合并处理。
 */
public class Pj05_OrderTx {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、分别获取两个流
        DataStreamSource<String> orderDS = env.readTextFile("input/OrderLog.csv");

        DataStreamSource<String> txDS = env.readTextFile("input/ReceiptLog.csv");

        //TODO 3、分别将两条数据转换成JavaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderDS.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
            }
        });

        SingleOutputStreamOperator<TxEvent> TxEventDS = txDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
            }
        });

        //TODO 4、使用connect将两条流连接起来
        ConnectedStreams<OrderEvent, TxEvent> orderEventAndTxEventDS = orderEventDS.connect(TxEventDS);

        //TODO 5、将相同交易码的数据聚合到一块
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = orderEventAndTxEventDS.keyBy("txId", "txId");

        //TODO 6、实时对账
        SingleOutputStreamOperator<String> result = orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {

            //创建一个Map集合用来存放订单表的数据
            HashMap<String, OrderEvent> orderMap = new HashMap<>();
            //创建一个Map集合用来存放交易表的数据
            HashMap<String, TxEvent> txMap = new HashMap<>();

            @Override
            public void processElement1(OrderEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //去看交易数据缓存里面是否存在当前订单id
                if (txMap.containsKey(value.getTxId())) {
                    //如果存在，则表示对账成功
                    out.collect(value.getTxId() + "对账成功");
                    //因为已对账，所以需要从缓存当中删除
                    txMap.remove(value.getTxId());
                } else {
                    //如果没有，则把当前订单加入订单缓存
                    orderMap.put(value.getTxId(), value);
                }
            }

            @Override
            public void processElement2(TxEvent value, KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {

                //去看订单数据缓存里面有没有当前交易订单Id
                if (orderMap.containsKey(value.getTxId())) {
                    //存在则说明对账成功
                    out.collect(value.getTxId() + "对账成功");
                    //因为对账成功，所有从订单缓存当中删除当前数据
                    orderMap.remove(value.getTxId());
                } else {
                    //订单缓存里面没有当前数据，则把交易数据写入
                    txMap.put(value.getTxId(), value);
                }

            }
        });

        result.print();

        env.execute("OrderTx");

    }

}
