package com.irving.index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 第二步
 * @Author yuanyc
 * @Date 14:19 2020-02-21
 */
public class SecondMr {

}

class ConcludeMap extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * 输入：xxx-a.txt 10
     *      xxx-b.txt 3
     * 输出：xxx a.txt->10
     *      xxx b.txt->3
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] arr = line.split("\t");

        String k = arr[0].split("-")[0];

        String v = arr[0].split("-")[1] + "->" + arr[1];

        context.write(new Text(k), new Text(v));
    }
}

/**
 * 整合Reduce
 * @Author yuanyc
 * @Date 10:52 2020-02-24
 */
class ConcludeReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuilder sb = new StringBuilder();
        for (Text text: values) {
            sb.append(text.toString());
            sb.append(",");
        }
        context.write(key, new Text(sb.toString()));
    }
}