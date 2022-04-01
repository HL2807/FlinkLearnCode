package com.lqs.five.part3_sink;

import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * @Author lqs
 * @Date 2022年03月29日 15:53:25
 * @Version 1.0.0
 * @ClassName Test03_ESSink
 * @Describe 将数据写入Elasticsearch
 */
public class Test03_ESSink {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转换为JSON格式
        SingleOutputStreamOperator<WaterSensor> StringToJsonDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(" ");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //TODO 4、将数据写入ES
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("nwh120",9200));
        httpHosts.add(new HttpHost("nwh121",9200));
        httpHosts.add(new HttpHost("nwh122",9200));

        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<>(httpHosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
                        //指定要插入的索引，类型，名docId
                        IndexRequest indexRequest = new IndexRequest("test_es_sink", "_doc", "1001");
                        String toJSONString = JSONObject.toJSONString(element);

                        //放入数据，显示声明为JSON字符串
                        indexRequest.source(toJSONString, XContentType.JSON);
                        //添加写入需求
                        indexer.add(indexRequest);
                    }
                }
        );

        //因为读的是无界数据流，ES会默认将数据先缓存起来，如果要实现来一条写一条，则将这个参数设置为1，
        //注意在生产当中尽量没有这门设置，数据量大不做缓存ES容易崩掉
        waterSensorBuilder.setBulkFlushMaxActions(1);
        StringToJsonDS.addSink(waterSensorBuilder.build());

        env.execute("ESSink");

    }

}
