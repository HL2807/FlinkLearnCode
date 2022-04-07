package com.lqs.flinksql.part6_udf_functions;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author lqs
 * @Date 2022年04月07日 19:50:19
 * @Version 1.0.0
 * @ClassName Test04_AggregateTableFunctions
 * @Describe 表聚合函数 多进多出
 */
public class Test04_AggregateTableFunctions {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        SingleOutputStreamOperator<WaterSensor> wordToOneDS = streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 3、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 4、将表转换为流
        Table table = tableEnvironment.fromDataStream(wordToOneDS);

        //TODO 不注册直接使用
//        table.groupBy($("id"))
//                .flatAggregate(call(MyUDTAF.class, $("vc")).as("value", "rank"))
//                .select($("id"), $("value"), $("rank"))
//                .execute()
//                .print();

        //TODO 注册后使用
        tableEnvironment.createTemporarySystemFunction("myUdtaf", MyUDTAF.class);
        table.groupBy($("id"))
                .flatAggregate(call("myUdtaf", $("vc")).as("value", "rank"))
                .select($("id"), $("value"), $("rank"))
                .execute().print();

//        tableEnvironment.executeSql(
//                "select id,myUdtaf(vc).as(value,rank) from "+table+" group by id"
//        ).print();

        env.execute();

    }

    /**
     * 自定义一个表聚合函数，多进多出，根据vc求Top2
     */
    public static class MyTopAcc {
        public Integer first;
        public Integer second;
    }

    public static class MyUDTAF extends TableAggregateFunction<Tuple2<Integer, String>, MyTopAcc> {

        @Override
        public MyTopAcc createAccumulator() {
            MyTopAcc acc = new MyTopAcc();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(MyTopAcc acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        public void emitValue(MyTopAcc acc, Collector<Tuple2<Integer, String>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, "1"));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, "2"));
            }
        }
    }

}
