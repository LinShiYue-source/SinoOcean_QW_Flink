package sinoocean.qw.func;

import sinoocean.qw.common.GmallConfig;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.*;
import java.util.Set;

import java.sql.DriverManager;

/**
 * Date : 2022-04-11 09:51:26
 * Description :
 */
public class DimSink extends RichSinkFunction<JSONObject> {

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    /**
     * 生成语句提交hbase
     * @param jsonObject
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        String tableName = jsonObject.getString("sink_table");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");
        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            String upsertSql = genUpsertSql(tableName.toUpperCase(), jsonObject.getJSONObject("data"));
            try {
                System.out.println(upsertSql);
                PreparedStatement ps = connection.prepareStatement(upsertSql);
                ps.executeUpdate();
                connection.commit();
                ps.close();
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("执行sql失败！");
            }
        }
    }

    public String genUpsertSql(String tableName, JSONObject jsonObject) {
        Set<String> fields = jsonObject.keySet();
        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" + StringUtils.join(fields, ",") + ")";
        String valuesSql = " values ('" + StringUtils.join(jsonObject.values(), "','") + "')";
        return upsertSql + valuesSql;
    }
}
