package com.lqs.five.part2_transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 22:16:24
 * @Version 1.0.0
 * @ClassName Test01_Repartition
 * @Describe 5.3.12    对流重新分区的几个算子
 * //先按照key分组，按照key的双重hash来选择后后面的分区
 * map.keyBy(value -> value).print("keyBy>>>>>>");
 * //对流中的元素随机进行分区
 * map.shuffle().print("shuffle>>>>>>");
 * //对流中的元素平均分布到每个区，当处理倾斜数据的时候，进行性能优化
 * map.rebalance().print("rebalance>>>>>>");
 * //同Rescale一样，也是平均循环的分布数据，但是要比Rebalance更高效，因为resale不需要
 * //通过网络，完全走的“管道”
 * map.rescale().print("rescale>>>>>>");
 */
public class Test11_Repartition_KeyBy_Shuffle_Reblance_Rescale {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、对数据做一个map操作
        SingleOutputStreamOperator<String> map = streamSource.map(value -> value).setParallelism(2);

        //TODO 4、调用对流重分区算子
        map.print("原始数据>>>>>>").setParallelism(2);
        //先按照key分组，按照key的双重hash来选择后后面的分区
        map.keyBy(value -> value).print("keyBy>>>>>>");
        //对流中的元素随机进行分区
        map.shuffle().print("shuffle>>>>>>");
        //对流中的元素平均分布到每个区，当处理倾斜数据的时候，进行性能优化
        map.rebalance().print("rebalance>>>>>>");
        //同Rescale一样，也是平均循环的分布数据，但是要比Rebalance更高效，因为resale不需要
        //通过网络，完全走的“管道”
        map.rescale().print("rescale>>>>>>");

        env.execute("Repartition");

    }

}
