import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
public class ConstructHTable {
public static void main(String[] args) throws IOException {
Configuration conf = HBaseConfiguration.create();
 HTable hTable = new HTable(conf, "-ROOT-");
System.out.println("Table is: " + 
Bytes.toString(hTable.getTableName()));
hTable.close();
}
}