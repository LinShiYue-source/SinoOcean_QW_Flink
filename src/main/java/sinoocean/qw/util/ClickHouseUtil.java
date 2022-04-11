package sinoocean.qw.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sinoocean.qw.common.GmallConfig;

import java.lang.reflect.Field;


/**
 * Date : 2022-04-11 14:22:45
 * Description :
 */
public class ClickHouseUtil {
    //获取针对ClickHouse的JdbcSink
    public static <T> SinkFunction getJdbcSink(String sql) {
        SinkFunction<T> sink = JdbcSink.<T>sink(
                sql,
                (jdbcPreparedStatement, t) -> {
                    Field[] fields = t.getClass().getDeclaredFields();
                    int skipOffset = 0; //
                    for (int i = 0; i < fields.length; i++) {
                        Field field = fields[i];
                        //通过反射获得字段上的注解
                        TransientSink transientSink =
                                field.getAnnotation(TransientSink.class);
                        if (transientSink != null) {
                            // 如果存在该注解
                            System.out.println("跳过字段：" + field.getName());
                            skipOffset++;
                            continue;
                        }
                        field.setAccessible(true);
                        try {
                            Object o = field.get(t);
                            //i代表流对象字段的下标，
                            // 公式：写入表字段位置下标 = 对象流对象字段下标 + 1 - 跳过字段的偏移量
                            // 一旦跳过一个字段 那么写入字段下标就会和原本字段下标存在偏差
                            jdbcPreparedStatement.setObject(i + 1 - skipOffset, o);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build());
        return sink;
    }

}
