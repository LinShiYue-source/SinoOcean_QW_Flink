package sinoocean.qw.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Date : 2022-04-15 13:15:55
 * Description :
 */
public class DimUtil {
    //直接从Phoenix中查询 没有缓存
    public static JSONObject getDimInfoWithNoCache(String tableName, Tuple2<String, String>... colNameAndValues) {
        //组合查询条件
        StringBuilder selectDimSql = new StringBuilder("select * from "+tableName+" where ");
        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;
            selectDimSql.append(colName +"='" +colValue +"'" );
            if(i < colNameAndValues.length - 1){
                selectDimSql.append(" and ");
            }
        }
//        System.out.println("查询维度SQL:" + selectDimSql);

        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据key关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据未找到:" + selectDimSql);
        }
        return dimInfoJsonObj;

    }

    public static void main(String[] args) {
        JSONObject dimInfoWithNoCache = DimUtil.getDimInfoWithNoCache("dim_action_type", Tuple2.of("ACTION_ID", "1"));
        System.out.println("DimUtil方法查询到的维度数据 ==》 " + dimInfoWithNoCache);
    }

}
