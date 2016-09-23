import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocumentHBaseMR {
	
	public static class MapperClass extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put>{
		
		public static final String[] columns={"url","media_doc_id","category","context"};
		
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
		Configuration conf = null; 
        conf=HBaseConfiguration.create();
        
        String[] libjarsArr = args[1].split(",");
        for (int i = 0; i < libjarsArr.length; ++i) {
        	addTmpJar(libjarsArr[i], conf);
        }
        
		conf.set("conf.column", "info");
		Job job =new Job(conf,"DocumentHBaseMR");		
		job.setJarByClass(DocumentHBaseMR.class);
		
		job.setMapperClass(Mapper.class);		
		job.setOutputFormatClass(TableOutputFormat.class);
		job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,"IndiaTable");
		
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setNumReduceTasks(0);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		 System.out.println(job.waitForCompletion(true) ? 0 : 1);
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


