package sinoocean.qw.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import sinoocean.qw.bean.UserAction;
import sinoocean.qw.bean.UserLogin;
import sinoocean.qw.bean.UserWide;
import sinoocean.qw.util.DimUtil;
import sinoocean.qw.util.MyKafkaUtil;
import sinoocean.qw.util.PhoenixUtil;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Date : 2022-04-14 14:16:39
 * Description :
 */
public class BaseDWM {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));


        //TODO 2.从Kafka的DWM DWD层的topic中读取数据
        String dwmTopic = "dwm_unique_visit";
        String dwdLoginTopic = "dwd_user_login";
        String dwdActionTopic = "dwd_user_action";
        String dwmUserWideTopic = "dwm_userWide";
        String groupId = "dwm_group";

//        FlinkKafkaConsumer<String> dwmSource = MyKafkaUtil.getKafkaSource(dwmTopic, groupId, "true");
//        DataStreamSource<String> dwmSourceDS = env.addSource(dwmSource);
//        SingleOutputStreamOperator<JSONObject> map = dwmSourceDS.map(new MapFunction<String, JSONObject>() {
//            @Override
//            public JSONObject map(String value) throws Exception {
//                return JSON.parseObject(value);
//            }
//        });
//        dwmSourceDS.print("Kafka DWM层数据流 ==》 ");

        FlinkKafkaConsumer<String> dwdLoginSource = MyKafkaUtil.getKafkaSource(dwdLoginTopic, groupId, "true");
        DataStream<String> dwdLoginSourceDS = env.addSource(dwdLoginSource);

        FlinkKafkaConsumer<String> dwdActionSource = MyKafkaUtil.getKafkaSource(dwdActionTopic, groupId, "true");
        DataStream<String> dwdActionSourceDS = env.addSource(dwdActionSource);
//        dwdSourceDS.print("Kafka DWD层数据流 ==》 ");

        //2.2 将读取到的数据流进行结构的转换 将String流转换为实体类流
        SingleOutputStreamOperator<UserLogin> userLoginDS = dwdLoginSourceDS.map(new MapFunction<String, UserLogin>() {
            @Override
            public UserLogin map(String value) throws Exception {
                return JSONObject.parseObject(value, UserLogin.class);
            }
        });

        SingleOutputStreamOperator<UserAction> userActionDS = dwdActionSourceDS.map(new MapFunction<String, UserAction>() {
            @Override
            public UserAction map(String value) throws Exception {
                return JSONObject.parseObject(value, UserAction.class);
            }
        });


        //TODO 3.设定事件时间水位
        SingleOutputStreamOperator<UserLogin> userLoginWithTimeDS = userLoginDS.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<UserLogin>() {
                    @Override
                    public long extractAscendingTimestamp(UserLogin element) {
                        return element.getLoginTime() * 1000L;
                    }
                }
        );
        SingleOutputStreamOperator<UserAction> userActionWithTimeDS = userActionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<UserAction>forMonotonousTimestamps().withTimestampAssigner(
                new SerializableTimestampAssigner<UserAction>() {
                    @Override
                    public long extractTimestamp(UserAction element, long recordTimestamp) {
                        return element.getActionTime() * 1000L;
                    }
                }
        ));

        //TODO 4.设置关联的key
        KeyedStream<UserLogin, String> userLoginTimeKeyDS = userLoginWithTimeDS.keyBy(new KeySelector<UserLogin, String>() {
            @Override
            public String getKey(UserLogin value) throws Exception {
                return value.getId();
            }
        });

        KeyedStream<UserAction, String> userActionTimeKeyDS = userActionWithTimeDS.keyBy(new KeySelector<UserAction, String>() {
            @Override
            public String getKey(UserAction value) throws Exception {
                return value.getId();
            }
        });

        //TODO 5.双流Join
        SingleOutputStreamOperator<UserWide> userWideDS = userLoginTimeKeyDS.intervalJoin(userActionTimeKeyDS)
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<UserLogin, UserAction, UserWide>() {
                    @Override
                    public void processElement(UserLogin userLogin, UserAction userAction, Context ctx, Collector<UserWide> out) throws Exception {
                        out.collect(new UserWide(userLogin, userAction));
                    }
                });
//        userWideDS.print("UserWide 合流后的数据流 ==》 ");

        //TODO 6.和维度数据进行关联
        SingleOutputStreamOperator<UserWide> userWideWithDimDS = userWideDS.map(new MapFunction<UserWide, UserWide>() {
            @Override
            public UserWide map(UserWide value) throws Exception {
                //获取当前数据的action_id
                Integer action_id = value.getAction_id();
                //根据当前action_id去维度表中获取actionType
                JSONObject dimActionType = DimUtil.getDimInfoWithNoCache("DIM_ACTION_TYPE", Tuple2.of("ACTION_ID", action_id.toString()));
                String action_name = dimActionType.getString("ACTION_NAME");
                //将从维度表中查询到的actionType赋值给宽流
                value.setActionName(action_name);

                Integer department_id = value.getDepartment_id();
                JSONObject dimDepartment = DimUtil.getDimInfoWithNoCache("DIM_DEPARTMENT", Tuple2.of("DEPARTMENT_ID", department_id.toString()));
                String department_name = dimDepartment.getString("DEPARTMENT_NAME");
                value.setDepartmentName(department_name);
                return value;
            }
        });

        userWideWithDimDS.print("Kafka的DWM层 ==》 ");

        //TODO 7.将处理好的流写到Kakfa的DWM层

        FlinkKafkaProducer<String> kafkaSink = MyKafkaUtil.getKafkaSink(dwmUserWideTopic);
        //转换流的结构
        SingleOutputStreamOperator<String> sinkDS = userWideWithDimDS.map(new MapFunction<UserWide, String>() {
            @Override
            public String map(UserWide value) throws Exception {
                return value.toString();
            }
        });

        sinkDS.addSink(kafkaSink);


        env.execute();
    }

}
