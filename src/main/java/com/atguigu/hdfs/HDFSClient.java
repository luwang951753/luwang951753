package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author lw
 * @Create2020-03-28 18:28
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS","hdfs://hadoop102:9000");

        //获取hdfs客户端对象
//        FileSystem fs = FileSystem.get(conf);

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
        //创建目录
        boolean mkdirs = fs.mkdirs(new Path("/0529/dashen/banzhan"));

        fs.close();

        System.out.println("ouver");


    }

    //文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        fs.copyFromLocalFile(new Path("banzhang.txt"),new Path("/xiaohua.txt"));

        fs.close();
    }

    //文件下载
    @Test
    public void testCopyToLocalhost()throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        fs.copyToLocalFile(new Path("/banhua.txt"),new Path("./banhuaaa.txt"));
//        fs.copyToLocalFile(false, new Path("/banhua.txt"),new Path("./banhubbb.txt"),true);
        fs.close();
    }

    //文件删除
    @Test
    public void testDelete()throws URISyntaxException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");
        //删除目录，递归
        fs.delete(new Path("/0529"),true);

        fs.close();

    }

    //文件更名
    @Test
    public void testRename()throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        fs.rename(new Path("/banhua.txt"),new Path("/hahaha.txt"));

        fs.close();
    }

    //文件详情
    @Test
    public void showFileInfo()throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()){
            LocatedFileStatus fileStatus = listFiles.next();

            //查看文件名称、权限、长度、块信息
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());

            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("------分割线------");

        }


        fs.close();
    }

    //判断文件还是文件夹
    @Test
    public void testFileStatus()throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "atguigu");

        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            if(fileStatus.isFile()){
                System.out.println("f:"+fileStatus.getPath().getName());
            }else{
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        fs.close();
    }
}
