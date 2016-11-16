package Htable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class LocalHBasePut {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		// ��hbase/conf/hbase-site.xml��hbase.zookeeper.quorum���õ�ֵ��ͬ
		conf.set("conf.column", "info");
		// ��hbase/conf/hbase-site.xml��hbase.zookeeper.property.clientPort���õ�ֵ��ͬ
		conf.set("hbase.zookeeper.property.clientPort", "31818");
		conf.set("hbase.rootdir", "hdfs://in-cluster/hbase");
		conf.set("hbase.zookeeper.quorum", "in-cluster-namenode1,in-cluster-namenode2,in-cluster-logserver");
		conf = HBaseConfiguration.create(conf);	
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf(args[0]));
		try {
			byte[] row = Bytes.toBytes(args[1]);
			Put put = new Put(row);
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("MediaDocId"), Bytes.toBytes(args[2]));
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("context"), Bytes.toBytes(args[3]));
			table.put(put);

		} finally {
			table.close();
			connection.close();
		}
	}

}
