package Htable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;

import DocProcess.CompositeDocSerialize;
import pipeline.CompositeDoc;

public class LocalHBaseGet {      
	
	public static void main(String[] args) throws IOException{	
		
        Configuration conf = new Configuration();            
        conf.set("conf.column", "info"); 
        
    	conf.set("hbase.zookeeper.property.clientPort", "31818"); //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");//与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同
        conf = HBaseConfiguration.create(conf);         
        HTable table = new HTable(conf,args[0]); 
        
        File f = new File(args[1]); //Read file 
        FileWriter writer = new FileWriter(args[2],true);//write result
    	BufferedReader br1=new BufferedReader (new InputStreamReader(new FileInputStream(f)));
    	
        String ID = null;
    	while ((ID = br1.readLine()) != null){
    		Get get = new Get(Bytes.toBytes(ID));  
    		Result rs = table.get(get); 	
    		byte[] value=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("context"));
    		byte[] value1=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("media_doc_id"));
    		String text=Bytes.toString(value1)+"\t"+Bytes.toString(value);
    		writer.write(text+"\n"); 
    		}
    	
    	if(writer != null){
    		writer.close();
    		}
	}
}
