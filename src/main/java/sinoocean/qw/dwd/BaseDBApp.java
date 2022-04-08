package sinoocean.qw.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.func.MyDeserializationSchemaFunction;
import sinoocean.qw.util.MyKafkaUtil;

/**
 * Date : 2022-04-08 15:23:22
 * Description :
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度
        env.setParallelism(3);
        /*
        //1.3检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/gmall/flink/checkpoint"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        */

        //TODO 2.从kafka主题中读取数据
        //2.1 定义主题以及消费者组
        String topic = "ods_base_db_m";
        String groupId = "basedbapp_group";
        //2.2 获取KafkaSource
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //2.3 对流的数据进行结构转换  String->JSONObject
        /*
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
            new MapFunction<String, JSONObject>() {
                @Override
                public JSONObject map(String jsonStr) throws Exception {
                    return JSON.parseObject(jsonStr);
                }
            }
        );
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(jsonStr -> JSON.parseObject(jsonStr));
        */

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //2.4 对数据进行简单的ETL
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        boolean flag = jsonObj.getString("table") != null
                                && jsonObj.getString("table").length() > 0
                                && jsonObj.getJSONObject("data") != null
                                && jsonObj.getString("data").length() > 3;

                        return flag;
                    }
                }
        );

        //filteredDS.print(">>>");
        //TODO 3.使用FlinkCDC读取配置表形成广播流
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop202")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall000_realtime")
                .tableList("gmall000_realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlDS.broadcast(mapStateDescriptor);

        //TODO 4.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filteredDS.connect(broadcastStream);

        //TODO 5.对数据进行分流操作  维度数据放到侧输出流  事实数据放到主流
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag"){};
        SingleOutputStreamOperator<JSONObject> realDS = connectedStream.process(new TableProcessFunction(dimTag,mapStateDescriptor));

        //获取维度侧输出流
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);

        realDS.print(">>>>");
        dimDS.print("####");


        env.execute();
    }

}
