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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class HBaseHashOutPut {
	
public static class HBaseToHdfsMapper extends TableMapper<Text,Text>{
	private Text outKey= new Text();
	//private LongWritable outKey = new LongWritable();
	private Text outValue=new Text();
	
	private String[] column_name = null;
	
	public void setup(Context context) {	
		column_name = context.getConfiguration().get("column_name").split(",");
	}
	
	protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
		//key is mean to rowkey
		byte[] media_doc_id=null;
		byte[] text=null;	
		
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < column_name.length; ++i) {
			String column_value = new String(value.getColumnLatestCell("info".getBytes(),column_name[i].getBytes()).getValue());
			sb.append(column_value);
			if (column_name.length != i + 1) {
				sb.append('\t');
			}
		}
		
		outKey.set(key.get());		
		outValue.set(sb.toString());
		context.write(outKey, outValue);	
	}
}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "31818");
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
        
        conf.set("column_name", args[1]);
               
		Job job=Job.getInstance(conf,HBaseHashOutPut.class.getSimpleName());
		job.setJarByClass(HBaseHashOutPut.class);
		job.setMapperClass(HBaseToHdfsMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
				
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),HBaseToHdfsMapper.class, Text.class,Text.class, job);
		
		//job.setOutputFormatClass(TextOutputFile.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}	
}
