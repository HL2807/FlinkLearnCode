package com.lqs.flinksql.part2_tableapi;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 09:45:46
 * @Version 1.0.0
 * @ClassName Test02_Demo_Aggregate
 * @Describe 基本使用，聚合操作
 */
public class Test02_Demo_Aggregate {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、获取数据
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60)
        );

        //TODO 3、获取表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //TODO 4、将流转换为表
        Table table = tableEnvironment.fromDataStream(waterSensorDataStreamSource);

        //TODO 5、通过连续查询获取数据
        Table resultTable = table.groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

        //TODO 6、将表转换流
        //TODO 使用追加流,注意：这里对数据进行了聚合操作，不能使用追加流
//        DataStream<Row> result = tableEnvironment.toAppendStream(resultTable, Row.class);
        //TODO 使用测回流
        DataStream<Tuple2<Boolean, Row>> result = tableEnvironment.toRetractStream(resultTable, Row.class);

        result.print();

        env.execute();

    }

}
