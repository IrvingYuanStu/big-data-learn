package com.irving.logClean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author yuanyc
 * @Description 日志清洗，只保留每行内容多余11个字段的的行
 * @Date 14:20 2020-02-17
 * 偏移量，行数据，输出key，输出value
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 行数据
        String val = value.toString();

        boolean flag = parseLog(val, context);
        if (!flag) {
            return;
        }

        Text t = new Text(val);
        context.write(t, NullWritable.get());
    }

    /**
     * 解析日志
     * @Author yuanyc
     * @Date 14:29 2020-02-17
     */
    private boolean parseLog(String val, Context context) {

        if (StringUtils.isBlank(val)) {
            context.getCounter("日志清洗", "非法数量").increment(1L);
            return false;
        }

        // 解析
        String[] arr = val.split(" ");
        if (null != arr && arr.length > 11) {
            context.getCounter("日志清洗", "合法数量").increment(1L);
            return true;
        }
        context.getCounter("日志清洗", "非法数量").increment(1L);
        return false;
    }
}
