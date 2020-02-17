package com.irving.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map执行类
 * 四个范型是map输入和输出的类型
 * @LongWritable 字符偏移量
 * @Author yuanyc
 * @Date 15:17 2019-07-11
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * 重写map方法
     * @Author yuanyc
     * @Date 15:19 2019-07-11
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 每行起始的偏移量
        System.out.println("-------");
        System.out.println("偏移量" + key.get());
        // 按行读取的数据
        String line = value.toString();
        // 根据空格切分str
        String[] arr = line.split(" ");
        // 对字符传标记1
        for (String str : arr) {
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
