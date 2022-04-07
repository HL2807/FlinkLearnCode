package com.lqs.flinksql.part2_tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author lqs
 * @Date 2022年04月07日 11:04:38
 * @Version 1.0.0
 * @ClassName Test03_ConnectorFileSource
 * @Describe 通过Connector声明读入数据,读取文件数据
 * 将动态表直接连接到数据
 */
public class Test03_ConnectorFileSource {

    public static void main(String[] args) {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 3、连接外部文件系统，获取数据
        Schema schema = new Schema();
        schema.field("id", DataTypes.STRING());
        schema.field("ts", DataTypes.BIGINT());
        schema.field("vc", DataTypes.INT());

        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().lineDelimiter("\n").fieldDelimiter(','))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        Table table = tableEnv.from("sensor");

        //TODO 5、通过连续查询获取数据
        Table resultTable = table.groupBy($("id"))
                .select($("id"), $("vc").sum().as("vcSum"));

//        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(resultTable, Row.class);
//
//        dataStream.print();

        //方式二，直接使用TableResult
        TableResult execute = resultTable.execute();
        execute.print();

        //如果没有调用流中的算子的话可以不同执行一下方法
//        env.execute();

    }

}
