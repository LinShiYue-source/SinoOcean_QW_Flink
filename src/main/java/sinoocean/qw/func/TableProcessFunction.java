package sinoocean.qw.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import javafx.scene.text.TextAlignment;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.common.QWConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Date : 2022-04-13 09:46:23
 * Description :
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection conn = null;
    //定义一个侧输出流标记
    private OutputTag<JSONObject> outputTag;

    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(QWConfig.PHOENIX_SERVER);
    }

    //通过构造方法给属性赋值
    public TableProcessFunction(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        //取出状态数据
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);


//        String table = value.getString("table");
//        String type = value.getString("type");
//        String data = value.getString("data");
//        //从状态中获取配置信息
//        String key = table + ":" + type;
//        TableProcess tableProcess = broadcastState.get(key);

        String table = value.getString("table");
        String type = value.getString("type");
        JSONObject dataJsonObj = value.getJSONObject("data");
        //从状态中获取配置信息
        String key = table ;
        TableProcess tableProcess = broadcastState.get(key);


        if (tableProcess != null) {
            //从配置表中有该条数据的配置
            value.put("sink_table", tableProcess.getSinkTable());
            if ("kafka".equals(tableProcess.getSinkType())) {
                //输出类型是kafka 写到主流
                out.collect(value);
            } else if ("hbase".equals(tableProcess.getSinkType())) {
                //输出类型是hbase 写到侧输出流
                ctx.output(outputTag, value);
            }

        } else {
            //配置表中没有该条数据的配置
            System.out.println("配置表中没有该条数据的配置" + key);
        }
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
        //将单条数据转换为json格式
        JSONObject jsonObject = JSON.parseObject(value);
        //获取其中的data
        String data = jsonObject.getString("data");
        //将data转换为tableProcess对象
        TableProcess tableProcess = jsonObject.parseObject(data, TableProcess.class);

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
//        String key = sourceTable + ":" + operateType;
        String key = sourceTable;


        //判断从当前配置表中读取到的配置信息是维度表还是事实表 如果是维度表，提前将维度表创建出来
//        if ("hbase".equals(sinkType) && "insert".equals(operateType)) {
//            checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
//        }

        //获取状态
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

        //将数据写入状态进行广播
        broadcastState.put(key, tableProcess);
    }

}
