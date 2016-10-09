import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DocumentFilterMR {
	
	public static class MapperClass extends Mapper<Object, Text, Text, Text>{
		
//		public static final String[] columns={"media_doc_id","hash"};
		
		private HashMap<String, String> hashMap=new HashMap<String,String>();
		private MessageDigest md;
		
		public void setup(Text value,Context context) throws NoSuchAlgorithmException, IOException{		
			md= MessageDigest.getInstance("MD5");	
			File file=new File(context.getConfiguration().get("IDList.txt"));
			BufferedReader br=new BufferedReader (new InputStreamReader(new FileInputStream(file)));
			String line=null;
			while((line = br.readLine()) != null) {
				String[] text=line.split("\t");
				hashMap.put(text[0],text[1]);
			}

		}		
		public void map(Object key, Text value, Context context) 
				throws IOException, InterruptedException{	
			String[] column=value.toString().split("\t");			
			md.update(column[2].getBytes());
			byte[] bytedata=md.digest();
			String str="";			
			for (int i=0;i<bytedata.length;i++){
				str += Integer.toString((bytedata[i] & 0xff) +0x100,16).substring(1);
				//����bytedata��0xff��λ�����㣬����8λ��0��ǿ��ת��Ϊbyte���ͣ�ת��16���ƣ���0x100����Ϊ�е�bytedata[i]��ʮ������ֻ��1λ  
			}
			
			if(hashMap.containsKey(column[0]) && hashMap.get(column[0])==str){
				
			} else {
				context.write(new Text(column[0]),new Text(str));
			}										
		}			
		
	}
	
	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException{

			Configuration conf=new Configuration();
			
		      String[] libjarsArr = args[2].split(",");
		        for (int i = 0; i < libjarsArr.length; ++i) {
		        	addTmpJar(libjarsArr[i], conf);
		        }
	      	Job job = new Job(conf, "DocumentFilterMR");
	    	job.setJarByClass(DocumentFilterMR.class);
	    	job.setMapperClass(Mapper.class);
	    	job.setNumReduceTasks(0);
	    	job.setOutputKeyClass(Text.class);
	    	job.setOutputValueClass(Text.class);
	    	
	    	FileInputFormat.addInputPath(job, new Path(args[0]));
	    	FileOutputFormat.setOutputPath(job, new Path(args[1]));

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


