import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocumentFilterMR {
    private static SequenceFile.Reader reader = null;  
    private static Configuration conf = new Configuration();  
    
	public static class MapperClass extends Mapper<Text , Text, Text, Text>{
		
//		public static final String[] columns={"media_doc_id","hash"};	
		private HashMap<String, String> hashMap=new HashMap<String,String>();
		private MessageDigest md;
		
		protected void setup(Context context)
                throws IOException, InterruptedException {
	        String dsf = context.getConfiguration().get("HDFS_hashlist");  
	        Configuration conf = new Configuration();      
	        FileSystem fs = FileSystem.get(URI.create(dsf),conf);  
	        FSDataInputStream hdfsInStream = fs.open(new Path(dsf));  
	             
	        String line;
	        while((line = hdfsInStream.readLine()) != null) {
	        	String[] items = line.split("\t");
	        	hashMap.put(items[0], items[1]);
	        	context.getCounter("custom", "hash_list_count").increment(1);
	        }
	        hdfsInStream.close();  
	        fs.close(); 
	        
	        try {
				md = MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
				
		public void map(Text   key, Text value, Context context) 
				throws IOException, InterruptedException {	

			
			
			String[] column=value.toString().split("\t");	
			md.update(column[1].getBytes());
			String key_string = key.toString();
			byte[] bytedata=md.digest();
			String str="";			
			for (int i=0;i<bytedata.length;i++){
				str += Integer.toString((bytedata[i] & 0xff) +0x100,16).substring(1);
				//����bytedata��0xff��λ�����㣬����8λ��0��ǿ��ת��Ϊbyte���ͣ�ת��16���ƣ���0x100����Ϊ�е�bytedata[i]��ʮ������ֻ��1λ  
			}
			
			if(hashMap.containsKey(key_string) && hashMap.get(key_string)==str){
				context.getCounter("custom", "processed_crawler_data").increment(1);
			} else {
				context.write(new Text(key_string),new Text(str));
			}						
		}			
		
	}
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{
		
			System.out.println("Start");

			Configuration conf=new Configuration();
			
			conf.set("HDFS_hashlist", args[0]);
			
	      	Job job = Job.getInstance(conf, "DocumentFilterMR");
	    	job.setJarByClass(DocumentFilterMR.class);
	    	job.setMapperClass(MapperClass.class);
	    	job.setNumReduceTasks(0);
	    	job.setInputFormatClass(SequenceFileInputFormat.class); //need the specific read method
	    	job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
	    	//job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	
	        FileSystem fs = FileSystem.get(conf);  
//	        reader = new SequenceFile.Reader(fs, new Path(args[1]), conf);  
	    	
	    	FileInputFormat.addInputPath(job, new Path(args[1]));
	    	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}


