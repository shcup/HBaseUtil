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
    	conf.set("hbase.zookeeper.property.clientPort", "31818"); //��hbase/conf/hbase-site.xml��hbase.zookeeper.property.clientPort���õ�ֵ��ͬ 
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");//��hbase/conf/hbase-site.xml��hbase.zookeeper.quorum���õ�ֵ��ͬ
        conf = HBaseConfiguration.create(conf);         
        HTable table = new HTable(conf,args[0]); 
        System.out.println("������ID:");
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
