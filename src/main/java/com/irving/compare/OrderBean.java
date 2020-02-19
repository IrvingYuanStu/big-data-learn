package com.irving.compare;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 需要排序的类
 * 实现comparable接口
 * @Author yuanyc
 * @Date 20:38 2020-02-18
 */
@Data
public class OrderBean implements WritableComparable<OrderBean> {
    // 订单ID
    private int id;

    // 商品id
    private String pId;

    // 成交金额
    private double price;

    public OrderBean() {

    }

    public OrderBean(int id, String pId, double price) {
        this.id = id;
        this.pId = pId;
        this.price = price;
    }

    @Override
    public int compareTo(OrderBean o) {
        // -1不换位置，1交换位置
        //根据ID从小到大排序，若ID相同则price从大到小排序
        if (this.getId() > o.getId()) {
            return 1;
        } else if (this.getId() < o.getId()) {
            return -1;
        } else {
            return this.price > o.getPrice() ? -1 : 1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(pId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.pId = dataInput.readUTF();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "id='" + id + '\'' +
                ", pId='" + pId + '\'' +
                ", price=" + price +
                '}';
    }
}
