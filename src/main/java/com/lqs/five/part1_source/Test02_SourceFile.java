package com.lqs.five.part1_source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author lqs
 * @Date 2022年03月28日 16:46:27
 * @Version 1.0.0
 * @ClassName Test02_SourceFile
 * @Describe 从文件读取数据
 */
public class Test02_SourceFile {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2、读取文件里面的数据
        env.readTextFile("input").print();

        env.execute("SourceFile");

        /**
         * 注意；
         * 1.	参数可以是目录也可以是文件
         * 2.	路径可以是相对路径也可以是绝对路径
         * 3.	相对路径是从系统属性user.dir获取路径: idea下是project的根目录, standalone模式下是集群节点根目录
         * 4.	也可以从hdfs目录下读取, 使用路径:hdfs://hadoop102:8020/...., 由于Flink没有提供hadoop相关依赖, 需要pom中添加相关依赖:
         * <dependency>
         *     <groupId>org.apache.hadoop</groupId>
         *     <artifactId>hadoop-client</artifactId>
         *     <version>3.1.3</version>
         * </dependency>
         */

    }

}
