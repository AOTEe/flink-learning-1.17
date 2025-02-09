package com.aootz.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UnboundedStreamOperatorChainDemo {
    public static void main(String[] args) throws Exception {

        // 1 创建执行环境
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(6);
        // 2 读取数据： socket linux：nc -lk 9000
        DataStreamSource<String> source = env.socketTextStream("192.168.93.136", 9000);
        // 3. 处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                })
                .setParallelism(2)// 设置算子平行度，并行度优先级：算子>执行环境>运行时指定
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)// 不设置并行度，本地模式默认为电脑的线程数
                .sum(1);

        // 4. 输出
        sum.print();

        // 5. 执行
        env.execute();
    }

    /**
     1、算子之间的传输关系：
     一对一
     重分区
     2、算子 串在一起的条件：
     1）一对一
     2）并行度相同
     3、关于算子链的api：
     1）全局禁用算子链：env.disableOperatorChaining();
     2）某个算子不参与链化：算子A.disableChaining(),算子A不会与 前面 和 后面 的算子 串在一起
     3）从某个算子开启新链条：算子A.startNewChain(),算子A不与 前面 串在一起，后面的可以串在一起



     什么时候禁用算子链（大多数情况下不需要禁用）
     1、各自算子链的运算任务很重
     2、定位问题，排查具体的算子的问题
     */
}
