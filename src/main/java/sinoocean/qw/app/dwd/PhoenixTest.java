package sinoocean.qw.app.dwd;

import com.alibaba.fastjson.JSONObject;
import sinoocean.qw.bean.ActionType;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.common.QWConfig;
import sinoocean.qw.func.TableProcessFunction;
import sinoocean.qw.util.PhoenixUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * Date : 2022-04-13 20:22:55
 * Description :
 */
public class PhoenixTest {
    public static void main(String[] args) throws Exception {

        List<JSONObject> actionTypes = PhoenixUtil.queryList("select * from QW_REALTIME.DIM_ACTION_TYPE", JSONObject.class);

        System.out.println(actionTypes);

    }

}
