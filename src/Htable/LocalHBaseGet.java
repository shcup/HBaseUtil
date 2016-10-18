package Htable;


import java.io.FileWriter;
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
        
		Get get = new Get(Bytes.toBytes(args[1]));  
 		if(get==null){
			System.out.println("The media_doc_id does not exist");
		}else{
		Result rs = table.get(get);  
		FileWriter writer=new FileWriter(args[2],true);	
		
		byte[] value=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("context"));
		byte[] value1=rs.getValue(Bytes.toBytes("info"),Bytes.toBytes("media_doc_id"));
//	     for(KeyValue kv : rs.raw()){  
//	    	 String s=new String(kv.getValue())+"\t";
		String s=Bytes.toString(value);
		String id=Bytes.toString(value1);
	 		writer.write(id+"\t"+s);
  
//	            System.out.print(new String(kv.getRow()) + " " );  
//	            System.out.print(new String(kv.getFamily()) + ":" );  
//	            System.out.print(new String(kv.getQualifier()) + " " );  
//	            System.out.print(kv.getTimestamp() + " " );  
//	            System.out.println(new String(kv.getValue()));  
//	        }
	     writer.write("\n");
	 		if(writer != null){  
                writer.close();     
            }
	}
	}

}
