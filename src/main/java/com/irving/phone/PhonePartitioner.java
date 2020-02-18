package com.irving.phone;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;

/**
 * 根据手机号所属运营商分区
 * @Author yuanyc
 * @Date 11:08 2020-02-18
 */
public class PhonePartitioner extends Partitioner<Text, NullWritable> {

    public static final String[] YD_ARR = new String[]{
            "134","135","136",
            "137","138","139",
            "150","151","152",
            "157","158","159",
            "188","187","182",
            "183","184","178",
            "147","172","198"
    };

    public static final String[] LT_ARR = new String[]{
            "130","131","132",
            "145","155","156",
            "166","171","175",
            "176","185","186","166"
    };

    public static final String[] DX_ARR = new String[]{
            "133","149","153",
            "173","177","180",
            "181","189","199"
    };

    /**
     * 重写父类方法
     * Text nullWritable 是map的输出类型
     */
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        String numStr = text.toString();

        String prefix = numStr.substring(0,3);

        if (Arrays.asList(YD_ARR).contains(prefix)) {
            // 移动 划分至分区0
            return 0;
        } else if (Arrays.asList(LT_ARR).contains(prefix)) {
            return 1;
        } else if (Arrays.asList(DX_ARR).contains(prefix)) {
            return 2;
        }
        return 0;
    }

}
