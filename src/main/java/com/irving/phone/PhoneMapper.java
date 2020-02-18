package com.irving.phone;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 手机号处理map
 * @Author yuanyc
 * @Date 11:18 2020-02-18
 */
public class PhoneMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        // 切分
        String[] phoneNums = line.split("\t");

        for (String num: phoneNums) {
            // 输出结果
            context.write(new Text(num), NullWritable.get());
        }
    }

}
