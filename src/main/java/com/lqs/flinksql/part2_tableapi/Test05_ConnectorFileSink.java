package com.lqs.flinksql.part2_tableapi;

import com.lqs.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 11:51:00
 * @Version 1.0.0
 * @ClassName Test05_connectorFileSink
 * @Describe 通过Connector声明写出数据
 * 写出到文件
 */
public class Test05_ConnectorFileSink {

    public static void main(String[] args) {

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
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 4、将流转为表
        Table tableInput = tableEnv.fromDataStream(waterSensorDataStreamSource);

        Table selectTable = tableInput.select($("id"), $("ts"), $("vc"));

        //TODO 5、连接外部文件系统，写入数据
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("output/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter('_'))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //将查询的数据写入表中，不支持测回操作
        selectTable.executeInsert("sensor");

    }

}
