package com.irving.logClean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) {

        Configuration configuration = new Configuration();

        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/web.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(LogDriver.class);

//            job.setMapperClass(LogMapper.class);
            job.setMapperClass(LogMapper2.class);

            // 指定输出范型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            // 不需要reduce, 设置reduce分区为0
            job.setNumReduceTasks(0);

            // 输入输出目录
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
