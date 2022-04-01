package com.lqs.six.littleproject;

import com.lqs.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月29日 18:12:54
 * @Version 1.0.0
 * @ClassName Pj04_Ads_Click
 * @Describe 各省份页面广告点击量实时统计
 *
 * 电商网站的市场营销商业指标中，除了自身的APP推广，还会考虑到页面上的广告投放（包括自己经营的产品和其它网站的广告）。
 * 所以广告相关的统计分析，也是市场营销的重要指标。
 *
 */
public class Pj04_AdsProvinceClick {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/AdClickLog.csv");

        //TODO 3、将数据转换为JavaBean并组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> provinceWihhAdIdToOneDS = streamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );

                return Tuple2.of(adsClickLog.getProvince() + "_" + adsClickLog.getAdId(), 1);
            }
        });

        //TODO 4、将相同的key聚合到一起并进行累加
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = provinceWihhAdIdToOneDS.keyBy(0)
                .sum(1);

        result.print();

        env.execute("Pj04_AdsProvinceClick");

    }

}
