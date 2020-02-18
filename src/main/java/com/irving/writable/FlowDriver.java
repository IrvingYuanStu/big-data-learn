package com.irving.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 统计不同手机号的上行、下行、总流量
 * "手机号 上行流量 下行流量 总流量"
 * @Author yuanyc
 * @Date 16:52 2020-02-18
 */
public class FlowDriver {

    public static void main(String[] args) {
        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/data/phone_data.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(FlowDriver.class);

            job.setMapperClass(FlowMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);

            job.setReducerClass(FlowReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
