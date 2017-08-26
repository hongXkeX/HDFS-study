package com.zkrt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsOperation {
	
	public static void main(String [] args) throws IOException, URISyntaxException {
        // 读取并统计各个单词出现次数的 top5
        String fileRead = "hdfs://192.168.71.111:9000/user/hadoop/when_you_old.txt";
        String statLine = ReadStatHDFS(fileRead, 5);

        System.out.println(statLine);
        // 将统计结果写回hdfs中的 top.txt 文件
        String fileWrite = "hdfs://192.168.71.111:9000/user/hadoop/top.txt";
        WriteToHDFS(fileWrite, statLine);
    }
	
	/**
	 * 读取指定文件并统计 top n 结果
	 * @param file  文件所在的URI
	 * @param top   指定top n的n值
	 * @return      返回表示统计结果的字符串
	 * @throws IOException
	 */
    public static String ReadStatHDFS(String file, Integer top) throws IOException {
        // key存放单词  value存放其出现的次数
        HashMap<String, Integer> hasWord = new HashMap<String, Integer>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        // 创建需访问的路径
        Path path = new Path(file);
        // 打开文件
        FSDataInputStream hdfsInStream = fs.open(path);
        // 创建缓冲流
        BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

        try {
        	// 逐行读取文件内容
            String line = br.readLine();
            while (line != null){
            	// 将大写全转换为小写 再用一些特定符号分离出单词
                String[] arrLine = line.toLowerCase().trim().split(",|:|;|[.]|[?]|!| ");
                // 循环处理一行中获得的单词
                for (int i = 0; i < arrLine.length; i++) {
                    String word = arrLine[i].trim();
                    if(word == null || word.equals("")){
                        continue;
                    }
                    // 若尚无此单词 新建一个key-1对
                    if (!hasWord.containsKey(word)) { 
                        hasWord.put(word, 1);
                    } else {  //如果有，就在将次数加1
                        Integer nCounts = hasWord.get(word);
                        hasWord.put(word, nCounts + 1);
                    }
                }
                // 再读取一行以循环遍历完整个文本
                line = br.readLine();
            }
        } catch (Exception e) {
        	e.printStackTrace();
        } finally {
            br.close();
            hdfsInStream.close();
            fs.close();
        }

        // 排序
        List<Map.Entry<String, Integer>> mapList = new ArrayList<Map.Entry<String, Integer>>(hasWord.entrySet());
        Collections.sort(mapList, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });

        //排序后
        String top_line = "";
        for(int i = 0; i < Math.min(mapList.size(), top); i++) {
            top_line = top_line + mapList.get(i).toString() + "\n";
        }

        return top_line;
    }
    
    /**
     * 在指定位置新建一个文件，并写入字符
     * @param  file
     * @param  words
     * @throws IOException
     * @throws URISyntaxException
     */
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataOutputStream out = fs.create(path);   //创建文件

        out.write(words.getBytes("UTF-8"));
        out.close();
    }
}
