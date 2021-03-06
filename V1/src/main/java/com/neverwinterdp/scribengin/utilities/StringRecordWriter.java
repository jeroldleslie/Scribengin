package com.neverwinterdp.scribengin.utilities;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StringRecordWriter {
  private FSDataOutputStream os;
  private FileSystem fs;

  public StringRecordWriter(String uri) throws IOException {
    Configuration conf = new Configuration();
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
    conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
    
    
    fs = FileSystem.get(URI.create(uri), conf);
    Path path = new Path(uri);

    //boolean flag = Boolean.getBoolean(fs.getConf().get("dfs.support.append"));
    //System.out.println("dfs.support.append is set to: " + flag);

    if (fs.exists(path)) {
      //System.out.println("!!!!!!! APPENDING to "+path.toString());
      os = fs.append(path);
    } else {
      //System.out.println("!!!!!!! CREATING "+path.toString());
      os = fs.create(path);
    }
  }

  public void write(byte[] bytes) throws IOException {
    os.write(bytes);
    os.write('\n');
  }

  public void close() {
    try {
      os.close();
    } catch (IOException e) {
      e.printStackTrace();
      //TODO: log
    }

    //try {
      //fs.close();
    //} catch (IOException e) {
      // TODO: log
    //}
  }
}
