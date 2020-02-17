package com.irving.logClean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 日志清洗，将个字段内容输出到类中
 * 194.237.142.21 - - [18/Sep/2013:06:49:18 +0000] "GET /wp-content/uploads/2013/07/rstudio-git3.png HTTP/1.1" 304 0 "-" "Mozilla/4.0 (compatible;)"
 * @Author yuanyc
 * @Date 15:23 2020-02-17
 */
public class LogMapper2 extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 行数据
        String line = value.toString();

        LogInfo info = parseLog(line, context);

        if (null != info && info.isValid()) {
            context.write(new Text(info.toString()), NullWritable.get());
        }

    }

    /**
     * 解析日志
     */
    private LogInfo parseLog(String line, Context context) {
        LogInfo info = null;

        String[] arr = line.split(" ");
        if (null != arr && arr.length > 11) {
            info = new LogInfo();

            info.setRemoteAddr(arr[0]);
            info.setTimeStamp(arr[3]);
            info.setRequest(arr[6]);
            info.setStatus(arr[8]);
            info.setValid(false);

            if (Integer.parseInt(info.getStatus()) == 200) {
                // 不等于200不采集
                info.setValid(true);
                context.getCounter("有效日志清洗", "有效请求数量").increment(1L);
            }

        } else {
            context.getCounter("有效日志清洗", "无效请求数量").increment(1L);
        }
        return info;
    }

}
