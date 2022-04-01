package com.lqs.six.littleproject;

import com.lqs.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author lqs
 * @Date 2022年03月29日 17:26:47
 * @Version 1.0.0
 * @ClassName Pj02_UV
 * @Describe 计算UV(Unique Visitor)每日用户独立总访问量
 * 说明：在pv案例中，我们统计的是所有用户对页面的所有浏览行为，也就是说，同一用户的浏览行为会被重复统计。
 * 而在实际应用中，我们往往还会关注，到底有多少不同的用户访问了网站，所以另外一个统计流量的重要指标是网站的独立访客数
 * （Unique Visitor，UV）
 */
public class Pj02_UV {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");

        //TODO 3、将数据转为实体类
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
            }
        });

        //TODO 4、过滤出PV数据
        SingleOutputStreamOperator<UserBehavior> pvDS = userBehaviorDS.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        //TODO 5、将数据组成Tuple2元组，其中key:"uv",value:"userId"
        SingleOutputStreamOperator<Tuple2<String, Long>> uvToUserIdDS = pvDS.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });

        //TODO 6、将相同的key的数据聚合到一起
        KeyedStream<Tuple2<String, Long>, Tuple> keyedStream = uvToUserIdDS.keyBy(0);

        //TODO 7、计算每日独立访问量
        keyedStream.process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String,Integer>>() {

            //第一个个存储器来对数据进行去重和存储
            HashSet<Long> set=new HashSet<Long>();

            @Override
            public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //将数据UserId插入set集合去重
                set.add(value.f1);
                //获取集合的元素个数
                int size = set.size();

                out.collect(Tuple2.of("uv",size));
            }
        }).print();

        env.execute("Pj02_UV");


    }

}
