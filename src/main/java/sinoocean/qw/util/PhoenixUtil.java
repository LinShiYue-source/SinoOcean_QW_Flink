package sinoocean.qw.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import sinoocean.qw.common.QWConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * Date : 2022-04-11 10:25:19
 * Description :
 */
public class PhoenixUtil {
    public static Connection conn = null;
    public static void main(String[] args) {
        List<JSONObject> objectList = queryList("select * from  base_trademark", JSONObject.class);
        System.out.println(objectList);
    }


    public static void queryInit() {
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            conn = DriverManager.getConnection(QWConfig.PHOENIX_SERVER);
            conn.setSchema(QWConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (conn == null) {
            queryInit();
        }
        List<T> resultList = new ArrayList();
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                T rowData = clazz.newInstance();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, md.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
            ps.close();
            rs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return resultList;
    }

}
