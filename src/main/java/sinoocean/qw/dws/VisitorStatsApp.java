package sinoocean.qw.dws;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sinoocean.qw.util.MyKafkaUtil;

/**
 * Date : 2022-04-11 10:58:23
 * Description :
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        String groupId = "visitor_stats_app";

        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);

        pageViewDStream.print("pv-------->");
        uniqueVisitDStream.print("uv=====>");
        userJumpDStream.print("uj--------->");
        env.execute();
    }


}
