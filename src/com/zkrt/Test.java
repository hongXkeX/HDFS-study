package com.zkrt;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

/**
 * 练习连接hdfs 读取内容
 * @author hongXkeX
 */
public class Test {
    public static void main(String[] args) throws IOException, URISyntaxException {
        //创建配置文件
    	Configuration conf = new Configuration();
        //创建需访问的路径
    	String path ="hdfs://192.168.71.111:9000/user/hadoop/when_you_old.txt";
        FileSystem fs = null;
        fs = FileSystem.get(URI.create(path),conf);
        //打开文件
        FSDataInputStream fsr  = fs.open(new Path(path));
        //创建缓冲流
    	BufferedReader reader = new BufferedReader(new InputStreamReader(fsr));
    	String lineTxt = null;
    	StringBuffer buffer = new StringBuffer();
    	//逐行读取文件内容
    	while ((lineTxt = reader.readLine()) != null) {
    		buffer.append(System.lineSeparator()+lineTxt);
		}
    	//输出
    	System.out.println(buffer.toString());
    	reader.close();
    	fsr.close();
        fs.close();
    }
}
