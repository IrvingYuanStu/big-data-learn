package com.irving.compare;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 分组排序
 * @Author yuanyc
 * @Date 10:56 2020-02-19
 */
public class GroupingComparator extends WritableComparator {

    public GroupingComparator() {
        super(OrderBean.class, true);
    }

    // 注意方法签名，错误的重写不会分组
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;

        // 根据id进行判断，返回0即认为key是同一类，不等式不同且进行排序
        if (oa.getId() > ob.getId()) {
            return 1;
        } else if (oa.getId() < ob.getId()) {
            return -1;
        }

        return 0;
    }
}
