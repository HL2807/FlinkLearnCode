package com.lqs.seven.part3_programstate;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @Author lqs
 * @Date 2022年04月05日 18:40:22
 * @Version 1.0.0
 * @ClassName Test08_StateBackend
 * @Describe 状态后端
 * 内存级别 MemoryStateBackend
 * 文件级别 FsStateBackend
 * RocksDB级别 RocksDBStateBackend
 */
public class Test08_StateBackend {

    public static void main(String[] args) throws IOException {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 设置状态后端
        //1.12版本写法
        //内存
        env.setStateBackend(new MemoryStateBackend());

        //文件系统
        env.setStateBackend(new FsStateBackend("hdfs://nwh120:8020/checkpoing/..."));

        //RocksDB
        env.setStateBackend(new RocksDBStateBackend("hdfs://nwh120:8020/checkpoing/rocksdb/..."));


        //新版本写法
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        //文件级别
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://nwh120:8020/checkpoing/..."));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://nwh120:8020/checkpoing/...");

        //RocksDB
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("hdfs://nwh120:8020/checkpoing/..."));
        env.getCheckpointConfig().setCheckpointStorage("hdfs://nwh120:8020/checkpoing/...");

        //checkpoing的barrier不对齐情况
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }

}
