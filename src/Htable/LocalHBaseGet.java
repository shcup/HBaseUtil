package Htable;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalHBaseGet {      
	
	public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();  
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.quorum配置的值相同   
        conf.set("conf.column", "info"); 
        //与hbase/conf/hbase-site.xml中hbase.zookeeper.property.clientPort配置的值相同 
    	conf.set("hbase.zookeeper.property.clientPort", "31818");
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
        conf = HBaseConfiguration.create(conf); 

//      HTable table=new HTable(conf,"Localltable");        
        HTable table=new HTable(conf,args[0]);
        
		Get get = new Get(args[1].getBytes());  
		Result rs = table.get(get);  
		
	     for(KeyValue kv : rs.raw()){  
	            System.out.print(new String(kv.getRow()) + " " );  
	            System.out.print(new String(kv.getFamily()) + ":" );  
	            System.out.print(new String(kv.getQualifier()) + " " );  
	            System.out.print(kv.getTimestamp() + " " );  
	            System.out.println(new String(kv.getValue()));  
	        }
	}

}
