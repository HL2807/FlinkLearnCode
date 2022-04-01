package com.lqs.six.littleproject;

import com.lqs.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author lqs
 * @Date 2022年03月29日 17:47:48
 * @Version 1.0.0
 * @ClassName Pj03_AppAnalysisByChanel
 * @Describe 市场营销商业指标统计分析
 * APP市场推广统计 - 分渠道
 */
public class Pj03_AppAnalysisByChanel {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、通过自定义数据源来获取数据
        DataStreamSource<MarketingUserBehavior> streamSource = env.addSource(new AppMarketingDataSource());

        //TODO 3、求不同渠道不同行为的个数
        SingleOutputStreamOperator<Tuple2<String, Integer>> channelWithBehaviorTOOneDS = streamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1);
            }
        });

        //TODO 4、对相同的key数据进行聚合
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = channelWithBehaviorTOOneDS.keyBy(0);

        //TODO 5、累计计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute("Pj03_AppAnalysisByChanel");

    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior>{

        //是否继续造数据标记
        boolean canRun = true;
        Random random = new Random();
        //生产数据的源配置
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {

            while (canRun){
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis()
                );

                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }

        }

        @Override
        public void cancel() {
            canRun=false;
        }
    }

}
