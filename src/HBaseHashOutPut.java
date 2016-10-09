import java.io.IOException;
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


public class HBaseHashOutPut {
	
public static class HBaseToHdfsMapper extends TableMapper<Text,Text>{
	private Text outKey= new Text();
	private Text outValue=new Text();
	
	protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
		//key is mean to rowkey
		byte[] media_doc_id=null;
		byte[] text=null;	
		
		media_doc_id=value.getColumnLatestCell("info".getBytes(),"media_doc_id".getBytes()).getValue();
		text=value.getColumnLatestCell("info".getBytes(),"context".getBytes()).getValue();
		
		outKey.set(key.get());
		String temp=(text == null || text.length==0)?"Null":new String(text);		
			outValue.set(temp);
			context.write(outKey, outValue);	
	}
}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "31818");
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
               
		Job job=Job.getInstance(conf,HBaseHashOutPut.class.getSimpleName());
		job.setJarByClass(HBaseHashOutPut.class);
		job.setMapperClass(HBaseToHdfsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),HBaseToHdfsMapper.class, Text.class,Text.class, job);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}	
}
