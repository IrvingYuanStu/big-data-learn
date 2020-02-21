package com.irving.join.reduce;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * ReduceJoin
 * @Author yuanyc
 * @Date 12:14 2020-02-20
 */
public class JoinReducer extends Reducer<LongWritable, TableBean, Text, NullWritable> {

    /**
     * @Param [values] 相同PID的集合
     */
    @Override
    protected void reduce(LongWritable key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        // 由于迭代器遍历以后不能再遍历，必须借助临时列表
        ArrayList<TableBean> list = new ArrayList<>();
        TableBean pdBean = new TableBean();

        try {
            // 输出到临时列表，并找到pd信息
            for (TableBean tableBean : values) {
                if (tableBean.getType() == 0L) {
                    // 订单信息
                    TableBean bean = new TableBean();
                    BeanUtils.copyProperties(bean, tableBean);
                    list.add(bean);
                } else {
                    // 品牌信息
                    BeanUtils.copyProperties(pdBean, tableBean);
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }

        // 输出结果
        for (TableBean bean : list) {
            String text = bean.getDate() + "\t" + pdBean.getName() + "\t" + bean.getCount();
            context.write(new Text(text), NullWritable.get());
        }
    }
}
