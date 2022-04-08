package sinoocean.qw.util;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

/**
 * Date : 2022-04-08 13:42:56
 * Description :
 */
public class MyKafkaUtil {
//    private static String kafkaServer = "hadoop002:9092,hadoop003:9092,hadoop004:9092";
    private static String kafkaServer = "10.0.24.116:9092";

    //封装Kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }

    //封装Kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }


}
