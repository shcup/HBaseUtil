package ExperimentDataExtract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
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

public class SentenceExtractorHindi {
	

	private static Map<String, String> normalizationMap = new HashMap<String, String>() {
		private static final long serialVersionUID = -5686972915633286599L;

		{
			put("\u0901", "\u0902"); // chandrabindu to anuswar conversion
			put("\u0964", ""); // purnaviram to empty character
			put("ऩ", "न");
			put("ऱ", "र");
			put("ऴ", "ळ");
			put("क़", "क");
			put("ख़", "ख");
			put("ग़", "ग");
			put("ज़", "ज");
			put("ड़", "ड");
			put("ढ़", "ढ");
			put("फ़", "फ");
			put("य़", "य");
			put("ॠ", "ऋ");
			put("ॡ", "ऌ");
		}
	};

	/**
	 * 
	 * @param input  - String that needs to be normalized
	 * @return noramlized string .. A normalized string here means replacing characters which have more than one
	 * 								representation to a single representation
	 */
	public static String NormalizedString(String input) {
		input = input.trim();
		int len = input.length();
		StringBuilder builder = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			String key = Character.toString(input.charAt(i));
			if (normalizationMap.containsKey(key)) {
				builder.append(normalizationMap.get(key));
			} else {
				builder.append(key);
			}
		}

		return builder.toString();
	}
	public static class HBaseSentenceExtractorMapper extends TableMapper<Text,Text>{
		private Text outKey= new Text();
		private Text outValue=new Text();
		
		protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
			//key is mean to rowkey
			byte[] media_doc_id=null;
			byte[] text=null;
			
			
			media_doc_id = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"media_doc_id".getBytes()));
			text = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"context".getBytes()));
			
			outKey.set(key.get());
			
			String temp=(text == null || text.length==0)?"Null":new String(text);		

			CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(temp, context);	
			outKey.set(compositeDoc.title);
			context.write(outKey, outValue);

			if (compositeDoc.main_text_list != null) {
				for (int i = 0; i < compositeDoc.main_text_list.size(); ++i) {
					String[] split_str = compositeDoc.main_text_list.get(i).split("[,?!;\u0964]");
					for (int j = 0; j < split_str.length; ++j) {
						String line = split_str[j].trim().replaceAll("[(){}\\[\\]\\\"]", "");
						line = NormalizedString(line);
						if (line.length() > 3 && line.contains(" ")) {
							outValue.set(line);
							context.write(outKey, outValue);
						}
						
					}
				}
			}			
		}
	}
	
	public static class HBaseSentenceFromWordsExtractorMapper extends TableMapper<Text,Text>{
		private Text outKey= new Text();
		private Text outValue=new Text();
		
		protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
			//key is mean to rowkey
			byte[] media_doc_id=null;
			byte[] text=null;
			
			
			media_doc_id = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"media_doc_id".getBytes()));
			text = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"context".getBytes()));
			
			outKey.set(key.get());
			
			String temp=(text == null || text.length==0)?"Null":new String(text);		

			CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(temp, context);	
			
			
			
			StringBuilder sb = new StringBuilder();
			for (String word : compositeDoc.title_words) {
				if (!word.equals("\t") && !word.equals(".")) {		
					sb.append(word);
					sb.append(' ');
				} else {
					outValue.set(sb.toString());
					context.write(outKey, outValue);
					sb.setLength(0);
				}
			}
			if (sb.length() != 0) {
				outValue.set(sb.toString());
				context.write(outKey, outValue);
				sb.setLength(0);
			}
			if (compositeDoc.body_words != null) {
				for (String word : compositeDoc.body_words) {
					if (!word.equals("\t") && !word.equals(".")) {		
						sb.append(word);
						sb.append(' ');
					} else {
						outValue.set(sb.toString());
						context.write(outKey, outValue);
						sb.setLength(0);
					}
				}
			}
			if (sb.length() != 0) {
				outValue.set(sb.toString());
				context.write(outKey, outValue);
				sb.setLength(0);
			}			
		}
	}
	
	public static class HBaseArticleExtractorMapper extends TableMapper<Text,Text>{
		private Text outKey= new Text();
		private Text outValue=new Text();
		
		protected void map(ImmutableBytesWritable key,Result value,Context context) throws IOException, InterruptedException{
			//key is mean to rowkey
			byte[] media_doc_id=null;
			byte[] text=null;
			
			
			media_doc_id = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"media_doc_id".getBytes()));
			text = CellUtil.cloneValue(value.getColumnLatestCell("info".getBytes(),"context".getBytes()));
			
			outKey.set(key.get());
			
			String temp=(text == null || text.length==0)?"Null":new String(text);		

			CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(temp, context);	
			
			StringBuilder sb = new StringBuilder();
			for (String word : compositeDoc.title_words) {
				if (!word.equals("\t") && !word.equals(".")) {		
					sb.append(word);
					sb.append(' ');
				}
			}
			if (compositeDoc.body_words != null) {
				for (String word : compositeDoc.body_words) {
					if (!word.equals("\t") && !word.equals(".")) {		
						sb.append(word);
						sb.append(' ');
					}
				}
			}
			outValue.set(sb.toString().trim());
			context.write(outKey, outValue);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		
		/*String input = "New Delhi, Oct 14 (ANI): The Bharatiya Janata Party (BJP) on Friday questioned the mentality of the Congress and Aam Aadmi Party (AAP) for objecting to the BRICS Summit logo and said it shows political bankruptcy on their part.\"The Aam Aadmi Party and Congress party objecting to the BRICS logo, which is lotus, again shows the bankruptcy. Now, there are many countries jointly they have discussed and they have decided to have a lotus now, what are the reasons I cannot say,";
		String res = input.replaceAll("[(){}\\[\\]\\\"]", "");*/

		// TODO Auto-generated method stub
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "31818");
        conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");   
        conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");

        if(args.length > 2){
        	String[] libjarsArr = args[2].split(",");
            for (int i = 0; i < libjarsArr.length; ++i) {
            	addTmpJar(libjarsArr[i], conf);
            }
        }
        

		Job job=Job.getInstance(conf,SentenceExtractorHindi.class.getSimpleName());
		job.setJarByClass(SentenceExtractorHindi.class);
		job.setMapperClass(HBaseArticleExtractorMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		
		job.setNumReduceTasks(0);
		TableMapReduceUtil.initTableMapperJob(args[0],new Scan(),HBaseArticleExtractorMapper.class, Text.class,Text.class, job);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}
		
	public static void addTmpJar(String jarPath, Configuration conf) throws IOException {
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		//String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String newJarPath = fs.makeQualified(new Path(jarPath)).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}
	
	
}
