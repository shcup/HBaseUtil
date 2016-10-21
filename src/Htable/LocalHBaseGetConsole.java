package Htable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;

import DocProcess.CompositeDocSerialize;
import pipeline.CompositeDoc;

public class LocalHBaseGetConsole {

	public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();            
        conf.set("conf.column", "info"); 
    	conf.set("hbase.zookeeper.property.clientPort", "31818"); //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");//与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同
        conf = HBaseConfiguration.create(conf);         
        HTable table = new HTable(conf,args[0]); 
        System.out.println("请输入ID:");
    	Scanner scan=new Scanner(System.in);
		while(scan.hasNext()){
			String s=scan.nextLine();			
    		Get get = new Get(Bytes.toBytes(s));  
    		Result rs = table.get(get); 	
    		byte[] value=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("context"));
    		byte[] value1=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("media_doc_id"));
    		String text=Bytes.toString(value);	
    		String title=null;
    		List<String> bodylist=new ArrayList<String>();
    		Context context=null;
    		CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(text, context);
    		title=compositeDoc.title;
    		bodylist=compositeDoc.body_words;  		
    		
    		System.out.println("title:\t"+title);
    		System.out.println("body:\t"+bodylist.toString());
		}

	}	
}
