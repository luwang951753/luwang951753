package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Author lw
 * @Create2020-03-29 11:25
 */
public class HDFSIO {

    //把本地的xxx.txt文件上传到hdfs根目录
    @Test
    public void putFileToHDFS() throws IOException, URISyntaxException, InterruptedException {

        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File("./banzhang.txt"));
        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banzhangsb.txt"));
        //4.流的对拷
        IOUtils.copyBytes(fis,fos,conf);
        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

    //从HDFS下载xxx.txt到本地
    //文件下载
    @Test
    public void testHdfsToLocal() throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        FSDataInputStream fis = fs.open(new Path("/banzhangsb.txt"));

        FileOutputStream fos = new FileOutputStream(new File("/tmp/ss.txt"));

        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();


    }

    //下载第一块
    @Test
    public void readFileSeek1()throws IOException, URISyntaxException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        FileOutputStream fos = new FileOutputStream(new File("/tmp/hadoop-2.7.2.tar.gz.part1"));

        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024*128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();


    }

    //下载第二块
    @Test
    public void readFileSeek2()throws IOException, URISyntaxException, InterruptedException {

        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        fis.seek(1024*1024*128);

        FileOutputStream fos = new FileOutputStream(new File("/tmp/hadoop-2.7.2.tar.gz.part2"));

        IOUtils.copyBytes(fis,fos,conf);

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();

    }

}
