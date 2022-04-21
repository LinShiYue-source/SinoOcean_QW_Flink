package sinoocean.qw.app.dws;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
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
import sinoocean.qw.bean.UserLogin;
import sinoocean.qw.bean.UserWide;
import sinoocean.qw.common.QWConfig;
import sinoocean.qw.util.ClickHouseUtil;
import sinoocean.qw.util.DimUtil;
import sinoocean.qw.util.MyKafkaUtil;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Date : 2022-04-15 13:55:28
 * Description :
 */
public class UVBaseDWS {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));


        //TODO 2.从kafka的DWM层中读取流
        String dwmTopic = "dwm_unique_visit";
        String groupId = "DWS_groupId";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(dwmTopic, groupId, "true");
        DataStreamSource<String> kafkaSourceDS = env.addSource(kafkaSource);


//        kafkaSourceDS.print("kafka的DWM层的数据 ==》 ");

        //TODO 3.转换流的格式
        //UV流的转换
        SingleOutputStreamOperator<UserLogin> userLoginDS = kafkaSourceDS.map(new MapFunction<String, UserLogin>() {
            @Override
            public UserLogin map(String value) throws Exception {
                return JSONObject.parseObject(value, UserLogin.class);
            }
        });

        //TODO 4.UV流关联维度数据
        SingleOutputStreamOperator<UserApp> userAppDS2 = userLoginDS.map(new MapFunction<UserLogin, UserApp>() {
            @Override
            public UserApp map(UserLogin value) throws Exception {
                Integer departmentId = value.getDepartmentId();
                JSONObject dimDepartment = DimUtil.getDimInfoWithNoCache("DIM_DEPARTMENT", Tuple2.of("DEPARTMENT_ID", departmentId.toString()));
                String department_name = dimDepartment.getString("DEPARTMENT_NAME");
                return new UserApp(
                        value.getDepartmentId(),
                        value.getLoginTime(),
                        department_name,
                        1L);
            }
        });
        userAppDS2.print("关联维度后的UV流 ==》");

        //TODO 4.设置时间语义
        SingleOutputStreamOperator<UserApp> userWideTimeDS = userAppDS2.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserApp>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<UserApp>() {
                            @Override
                            public long extractTimestamp(UserApp element, long recordTimestamp) {
                                return element.getLoginTime() * 1000L;
                            }
                        }
                ));

        //TODO 5.分组
        KeyedStream<UserApp, String> keyDS = userWideTimeDS.keyBy(new KeySelector<UserApp, String>() {
            @Override
            public String getKey(UserApp value) throws Exception {
                String departmentName = value.getDepartmentName();
                return departmentName;
            }
        });

        //TODO 6.开窗
        WindowedStream<UserApp, String, TimeWindow> windowDS = keyDS.window(TumblingEventTimeWindows.of(Time.seconds(3)));


        //TODO 7.聚合

        SingleOutputStreamOperator<UserApp> reduceDS = windowDS.reduce(new ReduceFunction<UserApp>() {

            public UserApp reduce(UserApp value1, UserApp value2) throws Exception {
                value1.setUvCount(value1.getUvCount() + value2.getUvCount());
                return value1;
            }
        });
        reduceDS.print("reduceDS ==> ");

        //TODO 8.将结果写到clickhouse
        reduceDS.addSink(ClickHouseUtil.getSinkFunction("insert into user_app values (?,?,?,?)"));

        env.execute();
    }

}

