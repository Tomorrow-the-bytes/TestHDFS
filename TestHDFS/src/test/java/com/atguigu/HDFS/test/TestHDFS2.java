package com.atguigu.HDFS.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFS2 {

	
	private FileSystem fs;
	   private FileSystem localFs;
		
		private Configuration conf = new Configuration();
		
		@Before
		public void init() throws IOException, URISyntaxException {
			
			//创建一个客户端对象
			 fs=FileSystem.get(new URI("hdfs://hadoop101:9000"),conf);
			 
			 localFs=FileSystem.get(new Configuration());
			
		}
		
		@After
		public void close() throws IOException {
			
			if (fs !=null) {
				fs.close();
			}
			
		}
		
		// 只上传文件的前10M
		/*
		 * 官方的实现
		 * InputStream in=null;
	      OutputStream out = null;
	      try {
	        in = srcFS.open(src);
	        out = dstFS.create(dst, overwrite);
	        IOUtils.copyBytes(in, out, conf, true);
	      } catch (IOException e) {
	        IOUtils.closeStream(out);
	        IOUtils.closeStream(in);
	        throw e;
	      }
		 */
	
		@Test
		public void testCustomUpload() throws Exception {
			//提供两个Path，和两个FileSystem
			Path path = new Path("D:/xxx.txt");
			   Path dest = new Path("/xxx.txt");
			// 使用本地文件系统中获取的输入流读取本地文件
			  FSDataInputStream is = localFs.open(path);
		
			// 使用HDFS的分布式文件系统中获取的输出流，向dest路径写入数据
			  FSDataOutputStream os = fs.create(dest, true);
			   
			// 1k
			byte[] buffer=new byte[1024];
			
			// 流中数据的拷贝
			
			for (int i=0; i<1024 * 10; i++) {
				is.read(buffer);
				os.write(buffer);
			}
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
	

		@Test
		public void testFirstBlock() throws Exception {
			Path src = new Path("/xxx/xxx.txt");
			Path dest = new Path("D:/firstblock");
			
			FSDataInputStream is = fs.open(src);
			FSDataOutputStream os = localFs.create(dest,true);
			
			byte[] buffer =new byte[1024];
			
			for(int i=0; i<1024 * 5;i++){
				is.read(buffer);
				os.write(buffer);
				
			}
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
			
		}
		@Test
		public void testSecondBlock() throws Exception {
			Path src = new Path("/xxx/xxx.txt");
			Path dest = new Path("D:/secondblock");
			
			FSDataInputStream is = fs.open(src);
			FSDataOutputStream os = localFs.create(dest,true);
			
			is.seek(1024*1024*5*1);
			
			byte[] buffer =new byte[1024];
			
			for(int i=0; i<1024 * 5;i++){
				is.read(buffer);
				os.write(buffer);
				
			}
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
		
		@Test
		public void testThridBlock() throws Exception {
			Path src = new Path("/xxx/xxx.txt");
			Path dest = new Path("D:/thridblock");
			
			FSDataInputStream is = fs.open(src);
			FSDataOutputStream os = localFs.create(dest,true);
			
			is.seek(1024*1024*5*2);
			
			byte[] buffer =new byte[1024];
			
			for(int i=0; i<1024 * 5;i++){
				is.read(buffer);
				os.write(buffer);
				
			}
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
		
		@Test
		public void testFourthBlock() throws Exception {
			Path src = new Path("/xxx/xxx.txt");
			Path dest = new Path("D:/fourthblock");
			
			FSDataInputStream is = fs.open(src);
			FSDataOutputStream os = localFs.create(dest,true);
			
			is.seek(1024*1024*5*3);
			
			byte[] buffer =new byte[1024];
			
			for(int i=0; i<1024 * 5;i++){
				is.read(buffer);
				os.write(buffer);
				
			}
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
		
		@Test
		public void testFifthBlock() throws Exception {
			Path src = new Path("/xxx/xxx.txt");
			Path dest = new Path("D:/fifthblock");
			
			FSDataInputStream is = fs.open(src);
			FSDataOutputStream os = localFs.create(dest,true);
			
			is.seek(1024*1024*5*4);
			
			IOUtils.copyBytes(is, os, 4096,true);
			
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
}
