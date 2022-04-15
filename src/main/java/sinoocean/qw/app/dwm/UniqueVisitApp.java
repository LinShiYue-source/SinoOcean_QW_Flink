package sinoocean.qw.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sinoocean.qw.bean.UserLogin;
import sinoocean.qw.util.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Date : 2022-04-14 10:48:15
 * Description :
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, Time.of(10, TimeUnit.SECONDS)));
        //TODO 1.从Kafka中读取数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_user_login";
        String sinkTopic = "dwm_unique_visit";

        //读取kafka数据
        FlinkKafkaConsumer<String> source = MyKafkaUtil.getKafkaSource(sourceTopic, groupId, "true");
        DataStreamSource<String> kafkaStream = env.addSource(source);

        //对读取的数据进行结构的转换
        DataStream<JSONObject> jsonObjStream = kafkaStream.map(jsonString -> JSON.parseObject(jsonString));
        jsonObjStream.print("Kafka DWD层用户登录数据流 ==》");

        //TODO 2.核心的过滤代码
        //按照用户id进行分组
        KeyedStream<JSONObject, String> keyByWithidDstream =
                jsonObjStream.keyBy(jsonObj -> jsonObj.getString("id"));


        SingleOutputStreamOperator<JSONObject> filteredJsonObjDstream =
                keyByWithidDstream.filter(new RichFilterFunction<JSONObject>() {
                    //定义状态用于存放最后访问的日期
                    ValueState<String> lastVisitDateState = null;
                    //日期格式
                    SimpleDateFormat simpleDateFormat = null;

                    //初始化状态 以及时间格式器
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        if (lastVisitDateState == null) {
                            //定义最后登录的状态
                            ValueStateDescriptor<String> lastViewDateStateDescriptor = new ValueStateDescriptor<>("lastViewDateState", String.class);
                            //因为统计的是当日UV，也就是日活，所有为状态设置失效时间
                            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                            //默认值 表明当状态创建或每次写入时都会更新时间戳
                            //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            //默认值  一旦这个状态过期了，那么永远不会被返回给调用方，只会返回空状态
                            //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
                            lastViewDateStateDescriptor.enableTimeToLive(stateTtlConfig);
                            lastVisitDateState = getRuntimeContext().getState(lastViewDateStateDescriptor);
                        }
                    }

                    //获取用户登录的时间
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {

                        String timeStamp = jsonObject.getString("login_time");
                        long timeStamp1 = Long.parseLong(timeStamp);
                        long l = timeStamp1 * 1000L;
                        String logDate = simpleDateFormat.format(l);
                        String lastViewDate = lastVisitDateState.value();
                        //获取用户的id
                        String id = jsonObject.getString("id");

                        if (lastViewDate != null && lastViewDate.length() > 0 && logDate.equals(lastViewDate)) {
                            //如果用户登录的时间和状态中的值一样 则表示已经登录过
                            System.out.println("已访问：用户id:" + id + " logDate：" + logDate);
                            return false;
                        } else {
                            // 如果不一样，状态值为空，则表示没有登录过，是今天的首次登录
                            System.out.println("未访问：用户id:" + id + " logDate：" + logDate);
                            lastVisitDateState.update(logDate);
                            return true;
                        }
                    }
                }).uid("uvFilter");

        SingleOutputStreamOperator<String> dataJsonStringDstream =
                filteredJsonObjDstream.map(jsonObj -> jsonObj.toJSONString());
        dataJsonStringDstream.print("过滤后的UV流 ==》");

        //将过滤后的UV流写到Kafka的DWM层的topic中
        dataJsonStringDstream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));


        env.execute();

    }
}
