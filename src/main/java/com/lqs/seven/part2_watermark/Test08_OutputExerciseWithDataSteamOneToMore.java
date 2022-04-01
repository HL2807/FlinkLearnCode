package com.lqs.seven.part2_watermark;

import com.alibaba.fastjson.JSONObject;
import com.lqs.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author lqs
 * @Date 2022年04月01日 22:19:28
 * @Version 1.0.0
 * @ClassName Test08_Output
 * @Describe 使用侧输出流把一个流拆成多个流
 * split算子可以把一个流分成两个流, 从1.12开始已经被移除了. 官方建议我们用侧输出流来替换split算子的功能.
 * 需求: 采集监控传感器水位值，将水位值高于5cm的值输出到side output
 */
public class Test08_OutputExerciseWithDataSteamOneToMore {

    public static void main(String[] args) throws Exception {

        //TODO 1、获取流数据执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //TODO 2、从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("nwh120", 8888);

        //TODO 3、将数据转为JavaBean类型
        SingleOutputStreamOperator<WaterSensor> wordToOneDS = streamSource.map(
                new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(" ");
                        return new WaterSensor(split[0], Long.parseLong(split[1])*1000, Integer.parseInt(split[2]));
                    }
                }
        );

        //TODO 4、将相同的id数据聚合到一起
        KeyedStream<WaterSensor, Tuple> keyedStream = wordToOneDS.keyBy("id");

        //获取侧输出流
        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("up5") {
        };

        OutputTag<WaterSensor> outputTag10 = new OutputTag<WaterSensor>("low10") {
        };

        //TODO 5、采集监控传感器水位值，将水位值高于5cm的值输出到侧输出流当中
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(
                new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        //将所有数据放到主流，另外单独将水位高于5cm放到侧输出流
                        if (value.getVc() > 5 && value.getVc()<10) {
                            //获取侧输出
                            ctx.output(outputTag, value);
                        }
                        //获取超过10cm的输出
                        else if (value.getVc()>=10){
                            ctx.output(outputTag10,value);
                        }
                        else if (value.getVc() <=5) {//将低于的放入主输出流
                            ctx.output(new OutputTag<WaterSensor>("low5") {
                            }, value);
                        }

                        out.collect(value);
                    }
                }
        );

        result.print("主流>>>>>>");

        DataStream<WaterSensor> sideOutput = result.getSideOutput(outputTag);

        //将获取的高于5cm的水位值转换成JSON数据后在输出
        SingleOutputStreamOperator<String> map = sideOutput.map(
                new MapFunction<WaterSensor, String>() {
                    @Override
                    public String map(WaterSensor value) throws Exception {
                        return JSONObject.toJSONString(value);
                    }
                }
        );

        map.print("水位高于5cm");

        //获取高于10cm的
        result.getSideOutput(outputTag10).print("水位高于10cm");

        env.execute("OutputExerciseWithDataSteamOneToMore");

    }

}
