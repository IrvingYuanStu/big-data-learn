package com.irving.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * 压缩API学习
 * @Author yuanyc
 * @Date 13:54 2020-02-19
 */
public class CompressDriver {

    public static void main(String[] args) throws Exception {

//        compress("/Users/yuanyc/Documents/workspace/hdfs/data/jvm_std.log", "org.apache.hadoop.io.compress.GzipCodec");

        decompress("/Users/yuanyc/Documents/workspace/hdfs/data/jvm_std.log.gz", ".txt");

    }

    /**
     * 压缩
     * @Author yuanyc
     * @Date 13:57 2020-02-19
     */
    public static void compress(String fileName, String handler) throws Exception {

        FileInputStream fis = new FileInputStream(new File(fileName));

        // 根据指定压缩算法，创建压缩工具类
        Class clazz = Class.forName(handler);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(clazz, new Configuration());

        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, cos, 1024 * 1024, true);

    }

    /**
     * 解压缩
     * 源文件 目标文件格式
     * @Author yuanyc
     * @Date 13:58 2020-02-19
     */
    public static void decompress(String fileName, String extension) throws Exception {

        // 创建factory示例
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));

        // 创建解压输入流
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(fileName));

        // 输出解压文件
        FileOutputStream fos = new FileOutputStream(new File(fileName + extension));
        IOUtils.copyBytes(cis, fos, 1024 * 1024, true);
    }
}
