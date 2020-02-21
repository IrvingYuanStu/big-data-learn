package com.irving.join.map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * MapJoin
 * @Author yuanyc
 * @Date 10:03 2020-02-20
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final String PD_PATH = "/Users/yuanyc/Documents/workspace/hdfs/data/join/pd.txt";

    // pid-名称缓存
    private Map<String, String> pdMap = new HashMap<>();

    /**
     * 初始化时执行, 加载小表到内存
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 先加载小表pd.txt
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(PD_PATH)));

        // 读取一行数据
        String line = null;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            String[] arr = line.split(" ");
            // 写入缓存
            pdMap.put(arr[0], arr[1]);
        }

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split(" ");
        String name = pdMap.get(arr[1]);
        // 输出结果
        String k = arr[0] + "\t" + name + "\t" + arr[2];
        context.write(new Text(k), NullWritable.get());
    }
}
