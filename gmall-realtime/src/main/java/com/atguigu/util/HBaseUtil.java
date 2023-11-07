package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

/**
 * @Author 城北徐公
 * @Date 2023/11/3-21:25
 */
public class HBaseUtil {

    /**
     * 获取配置连接
     * @return                 配置连接
     * @throws IOException     获取配置连接失败
     */
    public static Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        return ConnectionFactory.createConnection(configuration);
    }


    /**
     * 建表操作
     * @param connection     连接器
     * @param nameSpace      命名空间
     * @param table          表名
     * @param splitKeys      预分区键
     * @param cfs            列族
     * @throws IOException   建表失败抛异常
     */
    public  static void createTable(Connection connection,String nameSpace, String table, byte[][] splitKeys, String... cfs ) throws IOException {
        //判断列族是否存在
        int length = cfs.length;
        if (length<=0){
            throw  new RuntimeException("请设置至少一个列族！");
        }

        //判断表是否存在
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace + ":" + table);
        if (admin.tableExists(tableName)){
            System.out.println("待创建表"+table+"已经存在");
            return;
        }

        //将列族收集到集合中
        ArrayList<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        for (String cf : cfs) {
            ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes()).build();
            columnFamilyDescriptors.add(columnFamilyDescriptor);
        }

        //将列族集合放入描述器
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamilies(columnFamilyDescriptors)
                .build();

        //建表
        if (splitKeys==null){
            admin.createTable(tableDescriptor);
        }else {
            admin.createTable(tableDescriptor,splitKeys);
        }

        //关闭资源
        admin.close();
    }



    /**
     * 删表操作
     * @param connection     连接
     * @param nameSpace      命名空间
     * @param table          表名
     * @throws IOException   删表失败
     */
    public static void dropTable(Connection connection,String nameSpace,String table) throws IOException {
        //判断待删除表是否存在
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace + ":" + table);
        if (!admin.tableExists(tableName)){
            System.out.println("待删除表"+tableName+"不存在！");
            return;
        }else {
            //删表
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        //释放资源
        admin.close();

    }

    /**
     * 将与分区键切分后再转为字节数组
     * sinkExtend:"00|,01|,02|,03|..."
     * @param sinkExtend  预分区键
     * @return            处理预分区键(建表时需要的数据)
     */
    public static byte[][] getSplitKeys(String sinkExtend) {
        if (sinkExtend == null) {
            return null;
        } else {
            String[] split = sinkExtend.split(",");
            byte[][] splitKeys = new byte[split.length][];
            for (int i = 0; i < split.length; i++) {
                splitKeys[i] = split[i].getBytes();
            }
            return splitKeys;
        }
    }

    /**
     * 将预分区键和原本的rowKey组成新的rowKey
     * @param rowKey      原本的rowKey
     * @param sinkExtend  预分区键
     * @return            新的rowKey
     */
    public static String getRowKey(String rowKey, String sinkExtend) {
        String[] split = sinkExtend.split(",");
        ArrayList<String> extendList = new ArrayList<>();
        for (String s : split) {
            extendList.add(s.replace("|","_"));
        }
        extendList.add(split[split.length-1]);//多一个前缀,为了防止最后一个分区没有数据
        //预分区键拼接上原来的rowKey
        return extendList.get(Math.abs(rowKey.hashCode()% extendList.size()))+rowKey;
    }

    /**
     * Hbase中删除数据
     * @param connection      连接
     * @param hbaseNameSpace  命名空间
     * @param sinkTable       待删除数据的表名
     * @param rowKey          待删除数据的rowKey
     */
    public static void deleteData(Connection connection, String hbaseNameSpace, String sinkTable, String rowKey) throws IOException {
        //获取表对象
        Table table = connection.getTable(TableName.valueOf(hbaseNameSpace + ":" + sinkTable));
        //获取delete对象
        Delete delete = new Delete(rowKey.getBytes());
        //删除数据
        table.delete(delete);
        //关闭资源
        table.close();

    }

    /**
     * Hbase中一次性插入多条数据
     * @param connection        连接
     * @param hbaseNameSpace    命名空间
     * @param sinkTable         待插入数据的表名
     * @param rowKey            待插入数据的rowKey
     * @param columnFamily      待插入数据的列族
     * @param data              待插入的jsonObject类型数据,批量插入
     */
    public static void putJsonData(Connection connection, String hbaseNameSpace, String sinkTable, String rowKey, String columnFamily, JSONObject data) throws IOException {

        //获取table对象
        TableName tableName = TableName.valueOf(hbaseNameSpace + ":" + sinkTable);
        Table table = connection.getTable(tableName);

        //获取put对象
        Put put = new Put(rowKey.getBytes());

        //向put里面放入数据
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            Object value = entry.getValue();
            if (value!=null){
                //tops: 一般如果是add就可以放入多条数据,set是会覆盖写
                put.addColumn(columnFamily.getBytes(),entry.getKey().getBytes(),entry.getValue().toString().getBytes());
            }
        }

        //向表对象中放入put对象
        table.put(put);

        //关闭资源
        table.close();
    }


    /**
     * 向hbase中插入单条数据
     * @param connection  连接
     * @param nameSpace   命名空间
     * @param table       带插入数据表
     * @param rowKey      待插入数据行键
     * @param cf          列族
     * @param cn          列
     * @param value       待插入的单条数据
     * @throws IOException
     */
    public static void putData(Connection connection, String nameSpace, String table, String rowKey, String cf, String cn, String value) throws IOException {
        //获取表对象
        TableName tableName = TableName.valueOf(nameSpace + ":" + table);
        Table connectionTable = connection.getTable(tableName);
        //创建Put对象
        Put put = new Put(rowKey.getBytes());
        put.addColumn(cf.getBytes(), cn.getBytes(), value.getBytes());
        //执行插入数据操作
        connectionTable.put(put);
        //释放资源
        connectionTable.close();
    }

    /**
     *
     * @return
     */
    public static String getBaseDicDDL() {
        return "CREATE TABLE dim_base_dic (\n" +
                "    rowkey string,\n" +
                "    info ROW<dic_name string>\n" +
                ") WITH (\n" +
                "    'connector' = 'hbase-2.2',\n" +
                "    'table-name' = 'gmall_230524:dim_base_dic',\n" +
                "    'zookeeper.quorum' = 'hadoop102:2181'\n" +
                ")";
    }

}


