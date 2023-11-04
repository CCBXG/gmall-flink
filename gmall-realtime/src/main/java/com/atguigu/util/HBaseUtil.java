package com.atguigu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;

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
     * sinkExtend:"00|,01|,02|,03|..."
     * @param sinkExtend  预分区键
     * @return            处理预分区键
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

}
