package com.irving.phone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PhoneDriver {

    public static void main(String[] args) {
        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/phone.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(PhoneDriver.class);

            job.setMapperClass(PhoneMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 设置Partitioner
            job.setPartitionerClass(PhonePartitioner.class);

            // 设置分区数量
            // 少于实际产生报错，多余生成空文件
            job.setNumReduceTasks(3);

            // reduce输出类型
            job.setReducerClass(PhoneReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));


            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
