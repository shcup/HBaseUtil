package Htable;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalHBase {

	public static void main(String[] args) throws IOException {
        
		Configuration conf=HBaseConfiguration.create();
//		HConnection connection = null;
//		connection = HConnectionManager.createConnection(getHBaseConfiguration());
		conf.set("conf.column", "info");
		conf.set("hbase.zookeeper.property.clientPort", "31818");   
		
		   String[] libjarsArr = args[2].split(",");
	        for (int i = 0; i < libjarsArr.length; ++i) {
	        	addTmpJar(libjarsArr[i], conf);
	        }
		
        HTable table=new HTable(conf,"Localltable");
		
		byte[] row = Bytes.toBytes("1");
		Put put = new Put(row);
		put.add(Bytes.toBytes("info"),Bytes.toBytes("MediaDocId"),Bytes.toBytes("101"));
		put.add(Bytes.toBytes("info"),Bytes.toBytes("context"),Bytes.toBytes("hello world"));
			table.put(put);
			table.flushCommits();

		}
	public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}
	
}
