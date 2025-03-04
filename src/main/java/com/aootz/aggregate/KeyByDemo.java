package com.aootz.aggregate;

import com.aootz.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        /*
        1.返回一个KeyedStream
        2.KeyBy不是转换算子，只是对数据进行重分区，不能设置并行度
        3.KeyBy分组和分区的关系：
            1）KeyBy是对数据分组，保证相同key的数据在同一个分区
            2）分区：一个子任务可以理解为一个分区，一个分区（子任务）可以存在多个分组
         */
        DataStreamSource<WaterSensor> dataStreamSource = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s2", 1L, 1),
                new WaterSensor("s3", 1L, 1),
                new WaterSensor("s3", 1L, 1),
                new WaterSensor("s3", 1L, 1)
        );

        KeyedStream<WaterSensor, String> keyedStream = dataStreamSource.keyBy(WaterSensor::getId);
        keyedStream.print();

        env.execute();


    }
}
