package sinoocean.qw.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.func.DimSink;
import sinoocean.qw.func.MyDeserializationSchemaFunction;
import sinoocean.qw.func.TableProcessFunction;
import sinoocean.qw.util.MyKafkaUtil;

/**
 * Date : 2022-04-11 16:28:11
 * Description :
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2设置并行度 并行度数和kafka的partition数一致
        env.setParallelism(3);

        //1.3检查点相关的配置 精准一次性消费
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:8020/flink/checkpoint"));
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从kafka的ODS层主题中读取数据
        //2.1 定义主题以及消费者组
        String topic = "qw_ods_db";
        String groupId = "ods_group";
        //2.2 获取KafkaSource  从开始位置消费
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId, "true");
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(new MapFunction<String, JSONObject>() {

            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(value);
                return jsonObject;
            }
        });
        kafkaDS.print("kafka ODS层 ==》");
        //2.3 可以进行数据的清洗操作


        //TODO 3.使用FlinkCDC监控配置表的binlog
        DebeziumSourceFunction<String> proFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("qw_realtime")
                .tableList("qw_realtime.table_process")
                .deserializer(new MyDeserializationSchemaFunction())
//                .startupOptions(StartupOptions.initial()) // 监控全部binlog  包括历史的
                .startupOptions(StartupOptions.initial())  //增量监控binlog
                .build();

        DataStreamSource<String> proDS = env.addSource(proFunction);
        proDS.print("FlinkCDC 监控配置表 ==》");


        //3.2将配置表的流转换为广播流
        MapStateDescriptor<String, TableProcess> proDSDescriptor = new MapStateDescriptor<>("table-process", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = proDS.broadcast(proDSDescriptor);

        //TODO 4.连接主流和侧输出流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjectDS.connect(broadcastStream);

        //TODO 5.对数据进行分流
        //5.1定义侧输出流 用于放维度数据
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {
        };

        SingleOutputStreamOperator<JSONObject> processDS = connectedStream.process(new TableProcessFunction(dimTag, proDSDescriptor));
        //5.2获取维度侧输出流
        DataStream<JSONObject> hbaseDS = processDS.getSideOutput(dimTag);

        processDS.print("分流后 事实表 ==》");
        hbaseDS.print("分流后 维度表 ==》");

        //TODO 6.将侧输出流数据写入HBase(Phoenix)
        hbaseDS.print("DIM层写入维度表的数据");
        hbaseDS.addSink(new DimSink());

        //TODO 7.将主流数据写入Kafka
        processDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObj, @Nullable Long timestamp) {
                        //获取保存到Kafka的哪一个主题中
                        String topicName = jsonObj.getString("sink_table");
                        System.out.println("要写dwd层kafka的topic名 ==》 " + topicName);
                        //获取data数据
                        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
                        return new ProducerRecord<>(topicName,dataJsonObj.toString().getBytes());
                    }
                }
        ));



        env.execute();


    }

}
