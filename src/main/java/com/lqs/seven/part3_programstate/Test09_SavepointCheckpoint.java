package com.lqs.seven.part3_programstate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lqs
 * @Date 2022年04月05日 18:55:13
 * @Version 1.0.0
 * @ClassName Test09_SavepointCheckpoint
 * @Describe Checkpoint
 */
public class Test09_SavepointCheckpoint {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //设置用户权限
        System.setProperty("HADOOP_USER_NAME","lqs");

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://nwh120:8020/flinkTest/ck"));

        //开启ck
        //每5000ms开始一次Checkpoint
        env.enableCheckpointing(5000);
        //设置模式为精确一次（这是默认值）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //从ck位置恢复数据，在代码总开启cancel的时候不会删除checkpoint信息，这样就可以根据checkpoint来恢复数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //读取端口数据并转换为元组
        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDS = streamSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String s : value.split(" ")) {
                            out.collect(Tuple2.of(s, 1L));
                        }
                    }
                }
        );

        //TODO 3、按照单词分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordToOneDS.keyBy(tuple -> tuple.f0);

        //TODO 4、累加计算
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.sum(1);

        result.print();

        env.execute("SavepointCheckpoint");

    }

}
