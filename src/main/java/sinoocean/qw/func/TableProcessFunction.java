package sinoocean.qw.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


/**
 * Date : 2022-04-11 09:13:18
 * Description :
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    private Connection conn = null;
    //定义一个侧输出流标记
    private OutputTag<JSONObject> outputTag;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    //通过构造方法给属性赋值
    public TableProcessFunction(OutputTag<JSONObject> outputTag,MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化Phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if (type.equals("bootstrap-insert")) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //从状态中获取配置信息
        String key = table + ":" + type;
        TableProcess tableProcess = broadcastState.get(key);

        if (tableProcess != null) {
            jsonObj.put("sink_table", tableProcess.getSinkTable());
            if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                filterColumn(dataJsonObj, tableProcess.getSinkColumns());
            }
            if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                ctx.output(outputTag, jsonObj);
            } else if (tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                out.collect(jsonObj);
            }
        } else {
            System.out.println("NO this Key in TableProce" + key);
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //将单条数据转换为JSON对象
        JSONObject jsonObject = JSON.parseObject(value);
        //获取其中的data
        String data = jsonObject.getString("data");
        //将data转换为TableProcess对象
        TableProcess tableProcess = JSON.parseObject(data, TableProcess.class);

        //获取源表表名
        String sourceTable = tableProcess.getSourceTable();
        //获取操作类型
        String operateType = tableProcess.getOperateType();
        //输出类型      hbase|kafka
        String sinkType = tableProcess.getSinkType();
        //输出目的地表名或者主题名
        String sinkTable = tableProcess.getSinkTable();
        //输出字段
        String sinkColumns = tableProcess.getSinkColumns();
        //表的主键
        String sinkPk = tableProcess.getSinkPk();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();
        //拼接保存配置的key
        String key = sourceTable + ":" + operateType;

        //如果是维度数据，需要通过Phoenix创建表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {
            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //将数据写入状态进行广播
        broadcastState.put(key, tableProcess);
    }

    //拼接SQL，通过Phoenix创建表
    private void checkTable(String tableName, String fields, String pk, String ext) {
        if(pk == null){
            pk = "id";
        }
        if(ext == null){
            ext = "";
        }

        String[] fieldsArr = fields.split(",");
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists "+ GmallConfig.HBASE_SCHEMA +"."+tableName+"(");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            //判断当前字段是否为主键字段
            if(pk.equals(field)){
                createSql.append(field).append( " varchar primary key ");
            }else{
                createSql.append("info.").append(field).append( " varchar ");
            }
            //如果不是最后一个字段  拼接逗号
            if(i < fieldsArr.length - 1){
                createSql.append( ",");
            }
        }
        createSql.append(")");
        createSql.append(ext);
        System.out.println("Phoenix的建表语句：" + createSql);


        //执行SQL语句，通过Phoenix建表
        PreparedStatement ps = null;
        try {
            //创建数据库操作对象
            ps = conn.prepareStatement(createSql.toString());
            //执行SQL语句
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("在Phoenix中创建维度表失败");
        }finally {
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //对dataJsonObj中的属性进行过滤
    //dataJsonObj："data":{"id":12,"tm_name":"atguigu","logo_url":"/static/beijing.jpg"}
    //sinkColumns : id,tm_name
    private void filterColumn(JSONObject dataJsonObj, String sinkColumns) {
        String[] fieldArr = sinkColumns.split(",");
        //将数据转换为List集合，方便后面通过判断是否包含key
        List<String> fieldList = Arrays.asList(fieldArr);

        //获取json对象的封装的键值对集合
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();
        //获取迭代器对象   因为对集合进行遍历的时候，需要使用迭代器进行删除
        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
        //对集合中元素进行迭代
        for (;it.hasNext();) {
            //得到json中的一个键值对
            Map.Entry<String, Object> entry = it.next();
            //如果sink_columns中不包含  遍历出的属性    将其删除
            if(!fieldList.contains(entry.getKey())){
                it.remove();
            }
        }
    }
}
