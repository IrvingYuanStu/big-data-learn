package com.irving.join.reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinReduceMain {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/data/join/order.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        try {
            Job job = Job.getInstance(configuration);
            job.setJarByClass(JoinReduceMain.class);

            job.setMapperClass(RJoinMapper.class);
            // 指定输出范型
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(TableBean.class);

            job.setReducerClass(JoinReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            // 输入输出目录
            FileInputFormat.setInputPaths(job, new Path(args[0]), new Path("/Users/yuanyc/Documents/workspace/hdfs/data/join/pd.txt"));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
