package com.irving.join.reduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Reducejoin的map
 * @Author yuanyc
 * @Date 12:05 2020-02-20
 */
public class RJoinMapper extends Mapper<LongWritable, Text, LongWritable, TableBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 行数据
        String line = value.toString();
        String[] fields = line.split(" ");

        // 多个输入文件，根据文件名执行不同逻辑
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();

        // 设置标记位和key
        TableBean tableBean = new TableBean();
        long reduceKey = 0L;
        if (fileName.equals("order.txt")) {
            tableBean.setDate(fields[0]);
            tableBean.setPId(fields[1]);
            tableBean.setCount(Integer.parseInt(fields[2]));
            tableBean.setName("");   // 不能为null,否则序列化报错
            tableBean.setType(0L);

            reduceKey = Long.parseLong(fields[1]);
        } else {
            tableBean.setDate("");
            tableBean.setPId(fields[0]);
            tableBean.setCount(-1);
            tableBean.setName(fields[1]);
            tableBean.setType(1L);

            reduceKey = Long.parseLong(fields[0]);
        }

        context.write(new LongWritable(reduceKey), tableBean);
    }
}
