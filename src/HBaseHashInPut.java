import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class HBaseHashInPut {
	
	public static class MapperClass extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
		
		public static final String[] columns={"media_doc_id","hash"};
		
		public void map(LongWritable key,Text value,Context context) 
				throws IOException, InterruptedException{
			
			String[] columnVals=value.toString().split("\t");
			String rowkey=columnVals[0];
			Put put=new Put(rowkey.getBytes());
			for(int i=0;i<columnVals.length;i++){
				put.add("info".getBytes(),columns[i].getBytes(),columnVals[i].getBytes());
			}
			context.write(new ImmutableBytesWritable(rowkey.getBytes()), put);
		}			
		
	}
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{

			Configuration conf=HBaseConfiguration.create();
	     
			conf.set("conf.column", "info");
			
			conf.set("hbase.zookeeper.property.clientPort", "31818");
	        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");  
	        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
	        
	        conf.set(TableOutputFormat.OUTPUT_TABLE, "HashFilter");					
			Job job =new Job(conf,"HBaseHashInPut");
			TableMapReduceUtil.addDependencyJars(job);		
					
			job.setJarByClass(HBaseHashInPut.class);		
			job.setMapperClass(MapperClass.class);		
			job.setOutputFormatClass(TableOutputFormat.class);	
			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(LongWritable.class);
			
			job.setNumReduceTasks(0);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			System.out.println(job.waitForCompletion(true) ? 0 : 1);

	}

}


