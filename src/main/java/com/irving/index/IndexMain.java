package com.irving.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 多Job串联
 * @Author yuanyc
 * @Date 10:55 2020-02-24
 */
public class IndexMain {

    public static void main(String[] args) {
        // 目录
        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/data/index", "/Users/yuanyc/Documents/workspace/hdfs/out/index", "/Users/yuanyc/Documents/workspace/hdfs/out/result"};


        try {
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf);

            job1.setMapperClass(CountMap.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(IntWritable.class);

            job1.setReducerClass(CountReduce.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));


            Job job2 = Job.getInstance(conf);
            job2.setMapperClass(ConcludeMap.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setReducerClass(ConcludeReduce.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // 直接设置上个任务的输出文件夹，不需要具体到文件
            FileInputFormat.setInputPaths(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            ControlledJob ajob = new ControlledJob(job1.getConfiguration());
            ControlledJob bjob = new ControlledJob(job2.getConfiguration());

            JobControl control = new JobControl("index");
            // 连接任务
            bjob.addDependingJob(ajob);
            control.addJob(ajob);
            control.addJob(bjob);

            Thread t = new Thread(control);
            t.start();

            while(!control.allFinished()){
                Thread.sleep(1000);
            }
            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
