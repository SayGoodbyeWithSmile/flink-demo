package com.study.chuci;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理
 * @author yangjinhua
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件数据
        String inputPath = "/Users/yangjinhua/QuantGroup/GitLab/flink/flink-demo/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);
        //对数据集进行处理，按空格分词展开，转换成（word，1）二元组进行统计
        DataSet<Tuple2<String, Integer>> outDateSet = inputDataSet.flatMap(new MyFlatMap())
                // 按照第一个位置word进行分组
                .groupBy(0)
                // 按照第二个位置数据求和
                .sum(1);

        outDateSet.print();
    }

    // 自定义FlatMap实现 FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按空格分词
            String[] words = value.split(" ");
            // 遍历所有的word，包成二元组
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

}
