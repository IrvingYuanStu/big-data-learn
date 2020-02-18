package com.irving.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 流量统计Map
 * @Author yuanyc
 * @Date 16:34 2020-02-18
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        // 解析数据
        String[] arr = line.split("\t");
        long upFlow = Long.parseLong(arr[arr.length-3]);
        long downFlow = Long.parseLong(arr[arr.length-2]);
        String phoneNum = arr[1];

        // 输出结果
        context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow));
    }
}
