package com.lqs.five.part3_sink;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author lqs
 * @Date 2022年03月29日 16:16:17
 * @Version 1.0.0
 * @ClassName Test04_CustomizeSink
 * @Describe 自定义MySQL sink
 */
public class Test04_CustomizeSink {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换为JSON格式
        SingleOutputStreamOperator<WaterSensor> StringToJsonDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、将自定义Sink的数据写入MySQL
        StringToJsonDS.print("CustomizeSink>>>>>>");
        StringToJsonDS.addSink(new MySinkFunction());

        env.execute("CustomizeSink");

    }

    //自定义sink类
    public static class MySinkFunction extends RichSinkFunction<WaterSensor>{
        private Connection connection;
        private PreparedStatement pstm;

        //使用复函数声明周期方法优化连接


        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("创建连接......");
            //获取连接
            connection = DriverManager.getConnection(
                    "jdbc:mysql://nwh120:3306/test?useSSl=false",
                    "root",
                    "912811"
            );

            pstm = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //给占位符赋值
            pstm.setString(1,value.getId());
            pstm.setLong(2,value.getTs());
            pstm.setInt(3,value.getVc());

            pstm.execute();
        }

        @Override
        public void close() throws Exception {
            System.out.println("关闭连接......");
            pstm.close();
            connection.close();
        }
    }

}
