package com.irving.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 流量reducer
 * @Author yuanyc
 * @Date 16:41 2020-02-18
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {

    // key相同，value组成集合
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0L;
        long sumDownFlow = 0L;

        for (FlowBean bean : values) {
            sumUpFlow += bean.getUpFlow();
            sumDownFlow += bean.getDownFlow();
        }

        String result = sumUpFlow + " " + sumDownFlow + " " + (sumUpFlow + sumDownFlow);
        context.write(key, new Text(result));
    }
}
