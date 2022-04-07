package com.lqs.flinksql.part6_udf_functions;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author lqs
 * @Date 2022年04月07日 19:23:25
 * @Version 1.0.0
 * @ClassName Test02_TableFunctions
 * @Describe 表函数 一进多出
 */
public class Test02_TableFunctions {

    public static void main(String[] args) {

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
//        table.leftOuterJoinLateral(call(MyUDTF.class,$("id")))
//                .select($("id"),$("f0"))
//                .execute().print();

        //TODO 注册后使用
        tableEnvironment.createTemporarySystemFunction("myUDTF2",MyUDTF2.class);

//        table.leftOuterJoinLateral(call("myUDTF2"),$("id"))
//                .select($("id"),$("word"))
//                .execute().print();

        tableEnvironment.executeSql(
                "select id,word from "+table+" join lateral table(myUDTF2(id)) on true"
        ).print();

    }

    /**
     * 自定义一个表函数，一进多出，根据id按照下划线切分
     */
    //写法1
    public static class MyUDTF extends TableFunction<Tuple1<String>>{
        public void eval(String value){
            for (String word : value.split("_")) {
                collect(Tuple1.of(word));
            }
        }
    }

    //写法二
    @FunctionHint(output = @DataTypeHint("ROW<word string>"))
    public static class MyUDTF2 extends TableFunction<Row>{
        public void eval(String value){
            for (String word : value.split("_")) {
                collect(Row.of(word));
            }
        }
    }

}
