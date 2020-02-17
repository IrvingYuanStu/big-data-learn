package com.irving.wordcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义bean的hadoop序列化
 * @Author yuanyc
 * @Date 12:37 2019-07-14
 */
public class BeanWritable implements Writable, Comparable {

    private String name;
    private int age;

    /**
     * 序列化方法
     * @Author yuanyc
     * @Date 12:53 2019-07-14
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(name);
        dataOutput.writeInt(age);
    }

    /**
     * 反序列化，顺序要与序列化一致
     * @Author yuanyc
     * @Date 12:53 2019-07-14
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name = dataInput.readUTF();
        age = dataInput.readInt();
    }

    /**
     * 自定义对象用key时，需要重写compareto方法
     * @Author yuanyc
     * @Date 12:52 2019-07-14
     */
    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
