package com.atguigu.util;

/**
 * @Author 城北徐公
 * @Date 2023/11/6-10:09
 */

import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 任何JDBC框架的任何查询语句只有这四种情况
 * <p>
 * select count(*) from t;                     单行单列
 * select * from t where id ='xx'; id为主键     单行多列
 * select count(*) from t group by dept_id;    多行单列
 * select * from t;                            多行多列
 * <p>
 * id name sex
 * 10 zs   m
 * 11 ls   f
 * 12 ww   m
 */
public class JdbcUtil {
    /**
     *
     * @param connection jdbc连接
     * @param sql 要查询的sql语句
     * @param clz 结果集类型
     * @param isUnderScoreToCamel 是否要对属性列名格式转换  e.g.  hello_world -> helloWorld
     * @return 返回用户指定类型的查询结果数据集
     * @param <T> 用户自定义的数据集类型
     * @throws SQLException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T>List<T> queryList(Connection connection, String sql, Class<T> clz, boolean isUnderScoreToCamel) throws SQLException, InvocationTargetException, IllegalAccessException, InstantiationException {

        ArrayList<T> list = new ArrayList<>();
        //查询
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();  //结果集
        ResultSetMetaData metaData = resultSet.getMetaData();  //结果集的元数据(列名之类的信息)
        int columnCount = metaData.getColumnCount();  //结果集的列数

        //遍历结果集   行遍历
        while (resultSet.next()) {
            T t = clz.newInstance();
            //对每一个属性进行遍历    列遍历
            for (int i = 0; i < columnCount + 1; i++) {
                Object value = resultSet.getObject(i);
                String columnName = metaData.getColumnName(i);
                //蛇形转小驼峰
                if (isUnderScoreToCamel){
                    CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                //给T对象属性赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将T对象放入集合
            list.add(t);
        }

        //释放资源
        resultSet.close();
        preparedStatement.close();

        //返回结果
        return list;
    }
}
