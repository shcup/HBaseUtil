package Htable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper.Context;

import DocProcess.CompositeDocSerialize;
import pipeline.CompositeDoc;

public class LocalHBaseGetConsole {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("conf.column", "info");
		conf.set("hbase.zookeeper.property.clientPort", "31818"); // ��hbase/conf/hbase-site.xml��hbase.zookeeper.property.clientPort���õ�ֵ��ͬ
		conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
		conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");// ��hbase/conf/hbase-site.xml��hbase.zookeeper.quorum���õ�ֵ��ͬ
		conf = HBaseConfiguration.create(conf);
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(args[0]));
		System.out.println("������ID:");
		Scanner scan = new Scanner(System.in);
		try {

			while (scan.hasNext()) {
				String s = scan.nextLine();
				if (s.isEmpty()) {
					continue;
				}
				Get get = new Get(Bytes.toBytes(s));
				Result rs = table.get(get);
				if (rs == null || rs.size() == 0) {
					System.out.println(s + "\tEmpty Key!");
					continue;
				}
				byte[] value = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("context"));
				byte[] value1 = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("media_doc_id"));
				String text = Bytes.toString(value);
				List<String> bodylist = new ArrayList<String>();
				Context context = null;
				CompositeDoc compositeDoc = CompositeDocSerialize.DeSerialize(text, context);
				Print(compositeDoc);
			}
		} finally {
			scan.close();
			table.close();
			connection.close();
		}

	}

	public static void Print(CompositeDoc compositeDoc) {
		System.out.println("title:\t" + compositeDoc.title);
		System.out.println("URL:\t" + compositeDoc.doc_url);
		System.out.println("Domain:\t" + compositeDoc.media_doc_info.source);
		System.out.println("Language type:\t" + compositeDoc.language_type.toString());
		if (compositeDoc.writer != null) {
			System.out.println("Author:\t" + compositeDoc.writer.toString());
		}
		if (compositeDoc.category_info != null) {
			System.out.println("Category:\t" + compositeDoc.category_info.toString());
		}
		System.out.println("Show Time:\t" + compositeDoc.showtime);
		System.out.println("Content timestamp\t" + compositeDoc.media_doc_info.content_timestamp);
		System.out.println("Update timestamp\t" + compositeDoc.media_doc_info.update_timestamp);
		System.out.println("Crawler timestamp\t" + compositeDoc.media_doc_info.crawler_timestamp);
		System.out.println("Is headline\t" + compositeDoc.media_doc_info.is_headline);
		System.out.println("Is home\t" + compositeDoc.media_doc_info.is_home);
		System.out.println("Source name:\t" + compositeDoc.source_name);
		System.out.println("Source weight:\t" + compositeDoc.source_weight);
		System.out.println("Title:\t" + compositeDoc.title);
		System.out.println("Body:\t" + compositeDoc.main_text_list);
		System.out.println("Thumbnail:\t" + compositeDoc.img_text_list);
		System.out.println("Title words:\t" + compositeDoc.title_words);
		System.out.println("Body words:\t" + compositeDoc.body_words);
		System.out.println("Title NER\t" + compositeDoc.title_ner.toString());
		System.out.println("Body Ner\t" + compositeDoc.body_ner.toString());
		System.out.println("Title NP\t" + compositeDoc.title_np.toString());
		System.out.println("Body NP\t" + compositeDoc.body_np.toString());
		System.out.println("Title NNP\t" + compositeDoc.title_nnp.toString());
		System.out.println("Body NNP\t" + compositeDoc.body_nnp.toString());
		System.out.println("Text rank\t" + compositeDoc.text_rank.toString());
		System.out.println("Text rank phrase\t" + compositeDoc.text_rank_phrase.toString());
		String out;
		if (compositeDoc.media_doc_info.normalized_category_info == null) {
			out = "NULL";
		} else {
			out = compositeDoc.media_doc_info.normalized_category_info.toString();
		}
		System.out.println("Normalized category:\t" + out);
		if (compositeDoc.media_doc_info.classified_category_info == null) {
			out = "NULL";
		} else {
			out = compositeDoc.media_doc_info.classified_category_info.toString();
		}
		System.out.println("Classified category:\t" + out);
		// System.out.println();

		// System.out.println("body:\t"+compositeDoc.main_text_list.toString());
	}
}
