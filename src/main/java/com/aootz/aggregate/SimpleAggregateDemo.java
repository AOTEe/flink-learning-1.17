package com.aootz.aggregate;

import com.aootz.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleAggregateDemo {

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

        // index：Tuple类型才行，pojo不行
        //keyedStream.sum(2).print();
        // keyedStream.sum("ts").print();

        // max/maxBy区别：
        //      max:只会取key字段的最大值，非key字段保留第一次的值
        //      maxBy:取比较字段的最大值，同key字段取最大值的这条数据的值（first：存在相同key时，取原先的值，还是新的值）
        //keyedStream.max("ts").print();
        keyedStream.maxBy("ts", false).print();
        //keyedStream.print();

        env.execute();


    }
}
