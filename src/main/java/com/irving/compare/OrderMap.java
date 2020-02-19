package com.irving.compare;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 排序Map
 * MR排序都是针对key进行的
 * @Author yuanyc
 * @Date 20:54 2020-02-18
 */
public class OrderMap extends Mapper<LongWritable, Text, OrderBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] arr = line.split(" ");

        OrderBean ob = new OrderBean(Integer.parseInt(arr[0]), arr[1], Double.parseDouble(arr[2]));

        context.write(ob, new Text(ob.getPId()));
    }
}
