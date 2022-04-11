package sinoocean.qw.util;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Date : 2022-04-11 10:29:35
 * Description :
 */
public class DimUtil {
    //直接从Phoenix查询，没有缓存
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValue) {
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
        System.out.println("查询维度SQL:" + selectDimSql);
        JSONObject dimInfoJsonObj = null;
        List<JSONObject> dimList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);
        if (dimList != null && dimList.size() > 0) {
            //因为关联维度，肯定都是根据key关联得到一条记录
            dimInfoJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据未找到:" + sql);
        }
        return dimInfoJsonObj;
    }
    //先从Redis中查，如果缓存中没有再通过Phoenix查询 固定id进行关联
    public static JSONObject getDimInfo(String tableName, String id){
        return getDimInfo(tableName,Tuple2.of("id",id));
    }

    //先从Redis中查，如果缓存中没有再通过Phoenix查询 可以使用其它字段灵活关联
//Redis type:string维度的json字符串   key: dim:表:主键值_1_2      ttl:1天
    public static JSONObject getDimInfo(String tableName,Tuple2<String,String>...colNameAndValues){
        StringBuilder selectDimSql = new StringBuilder("select * from "+tableName+" where ");
        StringBuilder redisKey = new StringBuilder("dim:"+tableName.toLowerCase()+":");
        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;
            selectDimSql.append(colName +"='" +colValue +"'" );
            redisKey.append(colValue);
            if(i < colNameAndValues.length - 1){
                selectDimSql.append(" and ");
                redisKey.append("_");
            }
        }

        Jedis jedis = null;
        String jsonStr = null;
        JSONObject jsonObj = null;

        try {
            jedis = RedisUtil.getJedis();
            jsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Redis查询缓存异常");
        }

        if(jsonStr != null && jsonStr.length() > 0){
            jsonObj = JSON.parseObject(jsonStr);
        }else{
            System.out.println("查询维度的SQL:" + selectDimSql);
            List<JSONObject> jsonObjectList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);

            if(jsonObjectList!=null && jsonObjectList.size() > 0 ){
                jsonObj = jsonObjectList.get(0);
                if(jedis != null){
                    jedis.setex(redisKey.toString(),3600*24,jsonObj.toJSONString());
                }
            }else{
                System.out.println("维度数据没找到");
            }
        }

        if(jedis != null){
            jedis.close();
            System.out.println("--关闭Redis连接--");
        }
        return jsonObj;
    }
    //根据key让Redis中的缓存失效
    public static  void deleteCached( String tableName, String id){
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }


    public static void main(String[] args) {
        JSONObject dimInfooNoCache = DimUtil.getDimInfooNoCache("base_trademark", Tuple2.of("id", "13"));
        System.out.println(dimInfooNoCache);
    }

}
