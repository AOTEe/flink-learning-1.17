package com.aootz.aggregate;

import com.aootz.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 1),
                new WaterSensor("s2", 1L, 2),
                new WaterSensor("s3", 3L, 1),
                new WaterSensor("s3", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = dataStreamSource.keyBy(WaterSensor::getId);

        // reduce(value1,value2) value1:之前的计算结果（存状态），value2新来的数据
        keyedStream.reduce((item1, item2) -> new WaterSensor(item1.id, item1.ts, item1.vc + item2.vc)).print();

        env.execute();


    }
}
