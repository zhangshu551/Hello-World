package com.tjdata.spark.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;

/**
 * @author zhangshu
 * 2019年3月22日 上午10:14:34
 * @version 1.0
 */
public class ClassLoaderConfig extends ClassLoader {
	private static Object obj = new Object();
	private static ClassLoaderConfig loader = null;
	private static String classDir;
	private static FileSystem fs;
	private static Configuration conf;

	public static ClassLoaderConfig getInstance(String dir) {
		if (null == loader) {
			synchronized (obj) {
				if (null == loader) {
					classDir = dir;
					loader = new ClassLoaderConfig();
				}
			}
		}
		return loader;
	}

	private void read(InputStream input ,OutputStream out) throws IOException {
		int b = 0;
		while ((b = input.read()) != -1) {
			out.write(b);
		}
	}

	@Override
	protected Class<?> findClass(String name) {
		try {
			conf = new Configuration();
			fs = FileSystem.newInstance(conf);
			String clsPath = classDir + name.replace(".", "/") + ".class";
			FSDataInputStream input = fs.open(new Path(clsPath));
			ByteArrayOutputStream fout = new ByteArrayOutputStream();
			read(input, fout);
			byte[] byteArray = fout.toByteArray();
			input.close();
			fout.close();
			return defineClass(name, byteArray, 0, byteArray.length);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
			    if (null != fs) {
                    fs.close();
                    fs = null;
                }
                if (null != conf) {
			    	conf.clear();
			    	conf = null;
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}
}
