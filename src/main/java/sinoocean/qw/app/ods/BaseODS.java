package sinoocean.qw.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import sinoocean.qw.bean.TableProcess;
import sinoocean.qw.func.MyDeserializationSchemaFunction;
import sinoocean.qw.util.MyKafkaUtil;

/**
 * Date : 2022-04-12 15:11:27
 * Description :
 */
public class BaseODS {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //TODO 2.使用FlinkCDC读取 qw_realtime 库下业务表的binlog
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("qw_realtime")
                .tableList("qw_realtime.action_type", "qw_realtime.department", "qw_realtime.user_action", "qw_realtime.user_login")
                .deserializer(new MyDeserializationSchemaFunction())
               .startupOptions(StartupOptions.initial()) // 监控全部binlog  包括历史的
             //   .startupOptions(StartupOptions.latest())  //增量监控binlog
                .build();

        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        mysqlDS.print("FlinkCDC 监控MySQL业务表 ==》");


        //TODO 3.将监控到的业务表的binlog发送到kafka的ods层topic中
        String ods_topic = "qw_ods_db";
        FlinkKafkaProducer<String> odsKafkaSink = MyKafkaUtil.getKafkaSink(ods_topic);
        mysqlDS.addSink(odsKafkaSink);


//        mysqlDS.print();


        env.execute();
    }

}
