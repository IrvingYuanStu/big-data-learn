package com.irving.compare;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
    public static void main(String[] args) {
        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/data/GroupComparator.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(OrderDriver.class);

            job.setMapperClass(OrderMap.class);
            job.setMapOutputKeyClass(OrderBean.class);
            job.setMapOutputValueClass(Text.class);

            job.setReducerClass(OrderReduce.class);
            job.setOutputKeyClass(OrderBean.class);
            job.setOutputValueClass(NullWritable.class);

            // 注意设置分区
            job.setPartitionerClass(OrderPartitioner.class);
            job.setNumReduceTasks(3);

            // 设置辅助排序
            job.setGroupingComparatorClass(GroupingComparator.class);

            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
