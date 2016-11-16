package Htable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalHBaseGet {

	public static void main(String[] args) throws IOException {

		Configuration conf = new Configuration();
		conf.set("conf.column", "info");

		conf.set("hbase.zookeeper.property.clientPort", "31818"); // ��hbase/conf/hbase-site.xml��hbase.zookeeper.property.clientPort���õ�ֵ��ͬ
		conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
		conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");// ��hbase/conf/hbase-site.xml��hbase.zookeeper.quorum���õ�ֵ��ͬ
		conf = HBaseConfiguration.create(conf);
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(args[0]));
		try {
			// Use the table as needed, for a single operation and a single
			// thread
			File f = new File(args[1]); // Read file
			try (FileWriter writer = new FileWriter(args[2], true); // write result
																	
					BufferedReader br1 = new BufferedReader(new InputStreamReader(new FileInputStream(f)))) {
				String ID = null;
				while ((ID = br1.readLine()) != null) {
					Get get = new Get(Bytes.toBytes(ID));
					Result rs = table.get(get);
					byte[] value = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("context"));
					byte[] value1 = rs.getValue(Bytes.toBytes("info"), Bytes.toBytes("media_doc_id"));
					String text = Bytes.toString(value1) + "\t" + Bytes.toString(value);
					writer.write(text + "\n");
				}
			}

		} finally {
			table.close();
			connection.close();
		}

	}
}
