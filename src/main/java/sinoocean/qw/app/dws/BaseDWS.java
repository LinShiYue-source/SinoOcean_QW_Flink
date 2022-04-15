package sinoocean.qw.app.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sinoocean.qw.bean.UserApp;
import sinoocean.qw.bean.UserWide;
import sinoocean.qw.util.MyKafkaUtil;

import java.util.concurrent.TimeUnit;

/**
 * Date : 2022-04-15 13:55:28
 * Description :
 */
public class BaseDWS {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));


        //TODO 2.从kafka的DWM层中读取流
        String dwmTopic = "dwm_userWide";
        String groupId = "DWS_groupId";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(dwmTopic, groupId, "true");

        DataStreamSource<String> kafkaSourceDS = env.addSource(kafkaSource);

//        kafkaSourceDS.print("kafka的DWM层的数据 ==》 ");

        //TODO 3.转换流的格式
        SingleOutputStreamOperator<UserApp> mapDS = kafkaSourceDS.map(new MapFunction<String, UserApp>() {
            @Override
            public UserApp map(String value) throws Exception {
                return JSONObject.parseObject(value, UserApp.class);
            }
        });
        //TODO 4.设置时间语义
        SingleOutputStreamOperator<UserApp> userWideTimeDS = mapDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserApp>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<UserApp>() {
                            @Override
                            public long extractTimestamp(UserApp element, long recordTimestamp) {
                                return element.getActionTime() * 1000L;
                            }
                        }
                ));
        //TODO 5.分组
        KeyedStream<UserApp, Tuple2<String, String>> keyDS = userWideTimeDS.keyBy(new KeySelector<UserApp, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(UserApp value) throws Exception {
                String departmentName = value.getDepartmentName();
                String actionName = value.getActionName();
                return new Tuple2<>(departmentName, actionName);
            }
        });

        //TODO 6.开窗
        WindowedStream<UserApp, Tuple2<String, String>, TimeWindow> windowDS = keyDS.window(TumblingEventTimeWindows.of(Time.seconds(5)));


        //TODO 7.聚合
        SingleOutputStreamOperator<UserApp> reduceDS = windowDS.reduce(new ReduceFunction<UserApp>() {
            @Override
            public UserApp reduce(UserApp value1, UserApp value2) throws Exception {

                value1.setDianCount(1 + 1L);
                value1.setFangCout(1 + 1L);

                return value1;
            }
        });
        reduceDS.print("reduceDS ==> ");


        env.execute();
    }

}

