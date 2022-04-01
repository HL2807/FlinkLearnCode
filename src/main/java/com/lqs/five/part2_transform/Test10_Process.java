package com.lqs.five.part2_transform;

import com.lqs.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author lqs
 * @Date 2022年03月28日 22:00:41
 * @Version 1.0.0
 * @ClassName Test10_Process
 * @Describe process算子在Flink算是一个比较底层的算子,很多类型的流上都可以调用,可以从流中获取更多的信息(不仅仅数据本身)
 */
public class Test10_Process {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、使用process将数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDataStream = streamSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, ProcessFunction<String, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(" ");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        //TODO 4、对相同id使用keyBy聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDataStream.keyBy("id");

        //TODO 5、使用process将数据的VC做累加，实现类似Sum的功能
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {

            //存储结果
            private HashMap<String,Integer> map=new HashMap<>();

            @Override
            public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                //判断map集合中是否有对应的key值
                if (map.containsKey(value.getId())){
                    //key存在，则取出上一次累加过后的VC和
                    Integer lastVc = map.get(value.getId());
                    //拿上一次VC进行累加
                    int currentVcSum = lastVc + value.getVc();

                    //将当前的VC累加过后的结果重新写入map集合
                    map.put(value.getId(),currentVcSum);

                    //将数据发送到下游
                    out.collect(new WaterSensor(value.getId(),value.getTs(),currentVcSum));
                }else{
                    //可以不存在，则当前是第一条数据，直l接将当前数据存入map集合中饭即可
                    map.put(value.getId(),value.getVc());
                    out.collect(value);
                }
            }
        }).print();

        env.execute("Process");

    }

}
