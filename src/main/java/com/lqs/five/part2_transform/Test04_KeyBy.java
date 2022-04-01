package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 20:20:15
 * @Version 1.0.0
 * @ClassName Test04_KeyBy
 * @Describe keyBy分区算子
 * 作用：
 * 把流中的数据分到不同的分区中.具有相同key的元素会分到同一个分区中.一个分区中可以有多重不同的key.
 * 	在内部是使用的hash分区来实现的.
 */
public class Test04_KeyBy {

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

        streamSource.print("原始数据>>>>>>");

        //TODO 4、使用keyBy将相同id的数据聚合到一块
        //方式1：使用匿名实现类实现可以的选择器
        map.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        }).print("匿名实现类>>>>>>");

        //方式2：定义一个类实现KeySelector接口指定key
        map.keyBy(new MyKey()).print("定义类实现>>>>>>");

        //方式3：使用lambda表达式
        map.keyBy(value -> value.getId()).print("Lambda表达式>>>>>>");

        map.keyBy(WaterSensor::getId).print("Lambda表达式::>>>>>>");

        //方式4：通过属性名获取key，常用pojo
        map.keyBy("id").print("使用属性名获取>>>>>>");

        env.execute("KeyBy");

    }

    public static class MyKey implements KeySelector<WaterSensor,String>{

        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }

}
