package com.aootz.env;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvDemo {

    public static void main(String[] args) throws Exception {


        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "8082");//没生效啊...
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // env.setParallelism(30);
        // 2 读取数据： socket linux：nc -lk 9000
        DataStreamSource<String> source = env.socketTextStream("192.168.93.136", 9000);
        // 3. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                //.setParallelism(20)// 设置算子平行度，并行度优先级：算子>执行环境>运行时指定
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)// 不设置并行度，本地模式默认为电脑的线程数
                .sum(1);

        // 4. 输出
        sum.print();

        // 5. 执行
        env.execute();//后面的execute()会被阻塞

        // 异步执行
        //env.executeAsync();

    }
}
