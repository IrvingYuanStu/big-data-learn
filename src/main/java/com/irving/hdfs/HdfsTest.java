package com.irving.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;

/**
 * HDFS API学习
 */
public class HdfsTest {

    private static final String NAME_NODE_URI = "hdfs://192.168.56.101:9000";

    private static final String USER = "root";

    public static void main(String[] args) {

        FileSystem fileSystem = null;
        try {
//            listForder(fileSystem);
//            upload(fileSystem);
//            download(fileSystem);
//            mkDir(fileSystem);
//            rmDir(fileSystem);
//            streamDownLoad(fileSystem);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != fileSystem) {
                    fileSystem.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 上传文件
     */
    public static void upload(FileSystem fileSystem) throws Exception{
        System.out.println("测试上传文件");
        fileSystem = getFileSystem();

        // 源路径
        Path src = new Path("/Users/yuanyc/Desktop/soa.xml");
        // 目标路径
        Path dst = new Path("/");
        fileSystem.copyFromLocalFile(src, dst);
        System.out.println("上传文件结束");
    }

    /**
     * 打印目录信息
     */
    public static void listForder(FileSystem fileSystem) throws Exception{
        System.out.println("打印hdfs文件目录:");

        // 获取HDFS文件系统
        fileSystem = getFileSystem();

        Path path = new Path("/");
        RemoteIterator<LocatedFileStatus> it = fileSystem.listFiles(path, true);
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            System.out.println("文件名: " + status.getPath().getName());
            System.out.println("文件大小: " + status.getLen());
            System.out.println("块大小: " + status.getBlockSize());
            System.out.println("块位置: ");
            for (BlockLocation location : status.getBlockLocations()) {
                for (String host : location.getHosts()) {
                    System.out.println(host);
                }
            }
            System.out.println("全部信息:" + status);
            System.out.println("-------------------");
        }

        System.out.println("文件目录打印完毕");
    }

    /**
     * 下载文件
     */
    public static void download(FileSystem fileSystem) throws Exception {
        System.out.println("下载文件开始");
        fileSystem = getFileSystem();
        // 下载1.jpg
        Path src = new Path("/1.jpg");
        Path dst = new Path("/Users/yuanyc/Desktop");
        fileSystem.copyToLocalFile(src, dst);
        System.out.println("下载文件结束");
    }

    /**
     * 创建目录
     */
    public static void mkDir(FileSystem fileSystem) throws Exception {
        System.out.println("创建目录");

        fileSystem = getFileSystem();

        Path dir = new Path("/hello/world");
        fileSystem.mkdirs(dir);

        System.out.println("创建目录完成");
    }

    /**
     * 删除目录
     */
    public static void rmDir(FileSystem fileSystem) throws Exception {
        System.out.println("删除目录");

        fileSystem = getFileSystem();
        boolean isrecuirse = true;
        Path dir = new Path("/hello");
        fileSystem.delete(dir, isrecuirse);

        System.out.println("删除目录完成");
    }

    /**
     * 流方式下载
     */
    public static void streamDownLoad(FileSystem fileSystem) throws Exception {
        fileSystem = getFileSystem();

        Path path = new Path("/soa.xml");
        FSDataInputStream inputStream = fileSystem.open(path);


        IOUtils.copyBytes(inputStream, System.out, 1024*1024,false);
    }

    /**
     * 获取文件系统链接
     */
    private static FileSystem getFileSystem() throws Exception{
        // 支持自定义配置
        Configuration configuration = new Configuration();
        // NameNode的链接，client通过namenode获取元信息
        URI uri = new URI(NAME_NODE_URI);
        FileSystem fileSystem = FileSystem.get(uri, configuration, USER);
        return fileSystem;
    }
}
