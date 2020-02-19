package com.irving.compare;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 排序reduce
 * @Author yuanyc
 * @Date 20:59 2020-02-18
 */
public class OrderReduce extends Reducer<OrderBean, Text, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text t : values) {
            // TODO 可以直接获取topN
            sb.append(t);
            sb.append(",");
        }
        System.out.println("key=" + key.getId() + ", val=" + sb.toString());
        context.write(key, NullWritable.get());
        System.out.println("====================================");
    }
}
