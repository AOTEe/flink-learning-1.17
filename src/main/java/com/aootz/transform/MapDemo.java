package com.aootz.transform;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapDemo {

    public static void main(String[] args) {

        // Java Stream流

        List<String> list1 = Arrays.asList("1", "2", "3");
        List<String> list2 = Arrays.asList("4", "5", "6");


        //      map
        list2.stream().map(new Function<String, String>() {

            @Override
            public String apply(String s) {
                return null;
            }
        });

        //      flat map
        List<String> collect = list2.stream().flatMap(new Function<String, Stream<String>>() {
            @Override
            public Stream<String> apply(String s) {
                Stream<String> stream = Stream.<String>builder().add(s).add(s + "0").build();
                return stream;
            }
        }).collect(Collectors.toList());


        //
        System.out.println(collect);

        // 函数式接口
    }
}
