package sinoocean.qw.app.dwd;

import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.common.QWConfig;
import sinoocean.qw.func.TableProcessFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Date : 2022-04-13 20:22:55
 * Description :
 */
public class PhoenixTest {
    public static void main(String[] args) throws Exception {

        System.out.println("haha");
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//            Connection conn = DriverManager.getConnection(QWConfig.PHOENIX_SERVER);
            Connection conn = DriverManager.getConnection("jdbc:phoenix:192.168.88.102:2181");
//            PreparedStatement ps = conn.prepareStatement("upsert into bigdata.student values('1001','李四','beijing')");
            PreparedStatement ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS bigdata.student(\n" +
                    "id VARCHAR primary key,\n" +
                    "name VARCHAR,\n" +
                    "addr VARCHAR)\n");
            ps.execute();

    }

}
