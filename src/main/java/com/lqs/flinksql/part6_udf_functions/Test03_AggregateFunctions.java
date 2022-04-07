package com.lqs.flinksql.part6_udf_functions;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author lqs
 * @Date 2022年04月07日 19:38:06
 * @Version 1.0.0
 * @ClassName Test03_AggregateFunctions
 * @Describe 聚合函数 多进一出
 */
public class Test03_AggregateFunctions {

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
//                .select($("id"),call(MyUDAF.class,$("vc")))
//                .execute().print();

        //TODO 注册后使用
        tableEnvironment.createTemporarySystemFunction("myUDAF",MyUDAF.class);

//        table.groupBy($("id"))
//                .select($("id"),call(MyUDAF.class,$("vc")))
//                .execute().print();

        tableEnvironment.executeSql("select id,myUDAF(vc) from "+table+" group by id").print();

        env.execute();

    }

    /**
     * 自定义一个聚合函数，多进一出，求相同id的VC平均值
     */
    public static class MyAcc {
        public Integer vcSum;
        public Integer count;
    }

    public static class MyUDAF extends AggregateFunction<Double, MyAcc> {

        @Override
        public Double getValue(MyAcc accumulator) {
            return accumulator.vcSum * 1.0 / accumulator.count;
        }

        @Override
        public MyAcc createAccumulator() {
            MyAcc myAcc = new MyAcc();
            myAcc.vcSum = 0;
            myAcc.count = 0;
            return myAcc;
        }

        public void accumulate(MyAcc acc, Integer value) {
            acc.vcSum += value;
            acc.count++;
        }
    }

}
