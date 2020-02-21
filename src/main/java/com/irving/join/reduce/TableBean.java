package com.irving.join.reduce;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 表信息对象
 * @Author yuanyc
 * @Date 09:59 2020-02-20
 */
@Data
public class TableBean implements Writable {

    // 销售日期
    private String date;

    // 品牌ID
    private String pId;

    // 销量
    private int count;

    // 品牌名称
    private String name;

    // 类型，标记该bean是pd还是order
    private long type;

    public TableBean() {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(pId);
        dataOutput.writeInt(count);
        dataOutput.writeUTF(name);
        dataOutput.writeLong(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.date = dataInput.readUTF();
        this.pId = dataInput.readUTF();
        this.count = dataInput.readInt();
        this.name = dataInput.readUTF();
        this.type = dataInput.readLong();
    }
}
