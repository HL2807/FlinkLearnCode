package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 21:43:44
 * @Version 1.0.0
 * @ClassName Test08_MaxMaxBy
 * @Describe 滚动聚合算子
 * sum,min,max,minBy,maxBy
 * KeyedStream的每一个支流做聚合。执行完成后，会将聚合的结果合成一个流返回，所以结果都是DataStream
 * 注意:
 * 滚动聚合算子： 来一条，聚合一条
 * 1、聚合算子在 keyby之后调用，因为这些算子都是属于 KeyedStream里的
 * 2、聚合算子，作用范围，都是分组内。 也就是说，不同分组，要分开算。
 * 3、max、maxBy的区别：
 * max：取指定字段的当前的最大值，如果有多个字段，其他非比较字段，以第一条为准
 * maxBy：取指定字段的当前的最大值，如果有多个字段，其他字段以最大值那条数据为准；
 * 如果出现两条数据都是最大值，由第二个参数决定： true => 其他字段取 比较早的值； false => 其他字段，取最新的值
 */
public class Test08_MaxMaxBy {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、使用map将从端口读进来的字符串转为JavaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、先对相同的ID进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //TODO 5、使用聚合算子
        keyedStream.max("vc").print("max>>>>>>");
        keyedStream.maxBy("vc",true).print("maxByTrue");
        keyedStream.maxBy("vc",false).print("maxByFalse");

        env.execute("MaxAndMaxBy");

    }

}
