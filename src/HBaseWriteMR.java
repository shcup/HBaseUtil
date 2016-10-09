import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import DocProcess.CompositeDocSerialize;
import pipeline.CompositeDoc;

public class HBaseWriteMR {

	

public static class HBaseToHdfsMapper extends TableMapper<Text,Text>{
	private Text outKey= new Text();
	private Text outValue=new Text();
	
	private int fresh_limit = 0;
	
	  protected void setup(Context context)  
	          throws IOException, InterruptedException { 
		  fresh_limit = Integer.parseInt(context.getConfiguration().get("Fresh_limit"));
	  }
	
	protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
		//key is mean to rowkey
		byte[] media_doc_id=null;
		byte[] text=null;
		
		
		media_doc_id=value.getColumnLatestCell("info".getBytes(),"media_doc_id".getBytes()).getValue();
		text=value.getColumnLatestCell("info".getBytes(),"context".getBytes()).getValue();
		
		outKey.set(key.get());
//		String temp=((media_doc_id==null || media_doc_id.length==0)?"Null":new String(media_doc_id))+ "\t" + ((text == null || text.length==0)?"Null":new String(text));
		String temp=(text == null || text.length==0)?"Null":new String(text);
		System.out.println(temp);		
		
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(temp, context);
		
		long freshness = System.currentTimeMillis()/1000 - compositeDoc.media_doc_info.update_timestamp;
		/*outValue.set(String.valueOf(freshness) +
			" " + String.valueOf(fresh_limit * 24 * 3600));
		context.write(outKey, outValue);*/
		if(freshness < fresh_limit * 24 * 3600){
			outValue.set(CompositeDocSerialize.Serialize(compositeDoc, context));
			context.write(outKey, outValue);
		}
		
	
	}
}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "31818");
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
        
        conf.set("Fresh_limit", args[2]);
        
        String[] libjarsArr = args[3].split(",");
        for (int i = 0; i < libjarsArr.length; ++i) {
        	addTmpJar(libjarsArr[i], conf);
        }

		Job job=Job.getInstance(conf,HBaseWriteMR.class.getSimpleName());
		job.setJarByClass(HBaseWriteMR.class);
		job.setMapperClass(HBaseToHdfsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),HBaseToHdfsMapper.class, Text.class,Text.class, job);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

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
