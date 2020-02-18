package com.irving.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 输出
 * @Author yuanyc
 * @Date 11:41 2020-02-18
 */
public class PhoneReducer extends Reducer<Text, NullWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        context.write(key, new LongWritable(1L));
    }
}
