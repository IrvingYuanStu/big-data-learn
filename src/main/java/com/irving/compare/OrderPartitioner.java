package com.irving.compare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 排序分区，key-value是map结果类型
 * @Author yuanyc
 * @Date 22:10 2020-02-18
 */
public class OrderPartitioner extends Partitioner<OrderBean, Text> {

    /**
     * i是用户设定的分区数量
     * @Author yuanyc
     * @Date 22:12 2020-02-18
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        int id = orderBean.getId();
        return id % i;
    }
}
