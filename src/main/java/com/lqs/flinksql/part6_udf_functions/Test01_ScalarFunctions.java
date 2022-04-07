package com.lqs.flinksql.part6_udf_functions;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author lqs
 * @Date 2022年04月07日 19:08:28
 * @Version 1.0.0
 * @ClassName Test01_ScalarFunctions
 * @Describe 标量函数 一进一出
 * 用户定义的标量函数，可以将0、1或多个标量值，映射到新的标量值。
 * 为了定义标量函数，必须在org.apache.flink.table.functions中扩展基类Scalar Function，
 * 并实现（一个或多个）求值（evaluation，eval）方法。标量函数的行为由求值方法决定，求值方法必须公开声明并命名为eval
 * （直接def声明，没有override）。求值方法的参数类型和返回类型，确定了标量函数的参数和返回类型。
 */
public class Test01_ScalarFunctions {

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
//        table.select($("id"),call(MyUDF.class,$("id"))).execute().print();

        //TODO 注册后使用
        //注册自定义函数
        tableEnvironment.createTemporarySystemFunction("idLength",MyUDF.class);

        //注册后在使用
//        table.select($("id"),call("idLength",$("id"))).execute().print();

        tableEnvironment.executeSql("select id,idLength(id) from "+table).print();

        env.execute();

    }

    /**
     * 自定义一个标量函数，一进一出，获取输入id的字符串长度
     */
    public static class MyUDF extends ScalarFunction{
        public Integer eval(String value){
            return value.length();
        }
    }

}
