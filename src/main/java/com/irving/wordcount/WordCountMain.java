package com.irving.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 启动类
 * @Author yuanyc
 * @Date 15:39 2019-07-11
 */
public class WordCountMain {
    public static void main(String[] args) {

        Configuration configuration = new Configuration();
        args = new String[]{"/Users/yuanyc/Documents/workspace/hdfs/keytest.txt", "/Users/yuanyc/Documents/workspace/hdfs/out"};

        try {
            // 设置分隔符
            configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");
            configuration.set(NLineInputFormat.LINES_PER_MAP, "3");
            // 创建job
            Job job = Job.getInstance(configuration);
            job.setJarByClass(WordCountMain.class);

            // 指定map类
//            job.setMapperClass(WordCountMapper.class);
            job.setMapperClass(WordCountStringMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            // 指定reduce
            job.setReducerClass(WordCountRecuder.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//            // 设置combineformat对输入小文件优化
//            job.setinputformatclass(combinetextinputformat.class);
//            combinetextinputformat.setmaxinputsplitsize(job, 10 * 1024 * 1024);
//            combinetextinputformat.setmininputsplitsize(job, 5 * 1024 * 1024);


            job.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.setInputPaths(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 指定输入输出路径
//            FileInputFormat.setInputPaths(job, new Path(args[0]));
//            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            // 设置分片最大最小值
//            FileInputFormat.setMaxInputSplitSize(job, 10 * 1024 * 1024);
//            FileInputFormat.setMinInputSplitSize(job, 5 * 1024 * 1024);


            // 提交任务
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
