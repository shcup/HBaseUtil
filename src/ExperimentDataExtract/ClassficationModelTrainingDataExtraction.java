package ExperimentDataExtract;

import DocProcess.CompositeDocSerialize;
import DocProcessClassification.DataAdapter.*;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import pipeline.CompositeDoc;
import pipeline.basictypes.CategoryInfo;
import serving.mediadocinfo.MediaDocInfo;

public class ClassficationModelTrainingDataExtraction
{
 

  public static void addTmpJar(String jarPath, Configuration conf) throws IOException
  {
    System.setProperty("path.separator", ":");
    FileSystem fs = FileSystem.getLocal(conf);
    String newJarPath = new Path(jarPath).makeQualified(fs).toString();
    String tmpjars = conf.get("tmpjars");
    if ((tmpjars == null) || (tmpjars.length() == 0))
      conf.set("tmpjars", newJarPath);
    else
      conf.set("tmpjars", tmpjars + "," + newJarPath);
  }

  public static class HBaseClassificationModelTrainingDataExtractorMapper extends TableMapper<Text, Text>
  {
    private Text outKey = new Text();
    private Text outValue = new Text();

    private String adapter_name = null;
    private int category_level = 0;

    protected void setup(Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException
    {
      adapter_name = context.getConfiguration().get("adapter_name");
      category_level = Integer.parseInt(context.getConfiguration().get("category_level"));
    }

    protected void map(ImmutableBytesWritable key, Result value, Mapper<ImmutableBytesWritable, Result, Text, Text>.Context context) throws IOException, InterruptedException
    {
      byte[] media_doc_id = null;
      byte[] text = null;

      media_doc_id = value.getColumnLatestCell("info".getBytes(), "media_doc_id".getBytes()).getValue();
      text = value.getColumnLatestCell("info".getBytes(), "context".getBytes()).getValue();

      this.outKey.set(key.get());

      String temp = (text == null) || (text.length == 0) ? "Null" : new String(text);

      CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(temp, context);

      ClassifierInputTarget inputAdapter = null;
      if (adapter_name == "ClassifierNormalizedFeatureAdapter")
        inputAdapter = new ClassifierNormalizedFeatureAdapter();
      else if (adapter_name == "ClassifierNormalizedRichFeatureAdapter")
        inputAdapter = new ClassifierNormalizedRichFeatureAdapter();
      else if (adapter_name == "ClassifierOriginalFeatureAdapter")
        inputAdapter = new ClassifierOriginalFeatureAdapter();
      else if (adapter_name == "ClassifierOriginalWithCrawlerFeatureAdapter") {
        inputAdapter = new ClassifierOriginalWithCrawlerFeatureAdapter();
      }
      String res = inputAdapter.GetInputText(compositeDoc);
      this.outKey.set(compositeDoc.doc_url);
      String label;
      if ((compositeDoc.media_doc_info.normalized_category_info != null) && 
        (compositeDoc.media_doc_info.normalized_category_info.category_item != null) && 
        (compositeDoc.media_doc_info.normalized_category_info.category_item.size() != 0)) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < compositeDoc.media_doc_info.normalized_category_info.category_item.size(); i++) {
          sb.append(compositeDoc.media_doc_info.normalized_category_info.category_item.get(category_level));
          if (compositeDoc.media_doc_info.normalized_category_info.category_item.size() - 1 != i) {
            sb.append(",");
          }
        }
        label = sb.toString();
      } else {
        label = "NoMatch";
      }

      context.write(outKey, new Text(compositeDoc.media_doc_info.id + "\t" + label + "\t" + res));

      context.write(outKey, outValue);
    }
  }
  public static void main(String[] args)
		    throws IOException, ClassNotFoundException, InterruptedException
		  {
		    Configuration conf = HBaseConfiguration.create();
		    conf.set("hbase.zookeeper.property.clientPort", "31818");
		    conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
		    conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");

		    conf.set("adapter_name", args[0]);
		    conf.set("category_level", args[1]);

		    String[] libjarsArr = args[4].split(",");
		    for (int i = 0; i < libjarsArr.length; i++) {
		      addTmpJar(libjarsArr[i], conf);
		    }

		    Job job = Job.getInstance(conf, ClassficationModelTrainingDataExtraction.class.getSimpleName());
		    job.setJarByClass(ClassficationModelTrainingDataExtraction.class);
		    job.setMapperClass(HBaseClassificationModelTrainingDataExtractorMapper.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);

		    job.setNumReduceTasks(0);
		    TableMapReduceUtil.initTableMapperJob(args[2], new Scan(), HBaseClassificationModelTrainingDataExtractorMapper.class, Text.class, Text.class, job);

		    job.setOutputFormatClass(TextOutputFormat.class);
		    FileOutputFormat.setOutputPath(job, new Path(args[3]));

		    job.waitForCompletion(true);
		  }
}