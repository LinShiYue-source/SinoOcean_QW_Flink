package sinoocean.qw.util;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import sinoocean.qw.common.QWConfig;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Date : 2022-04-20 16:11:40
 * Description :
 */
public class ClickHouseUtil {
    //获取针对ClickHouse的JdbcSink
    public static <T> SinkFunction<T> getSinkFunction(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        //给占位符赋值
                        Field[] fields = obj.getClass().getDeclaredFields();
                        //对属性数组进行遍历
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            //设置私有属性的访问权限
                            field.setAccessible(true);
                            try {
                                //获取属性的值
                                Object o = field.get(obj);
                                //给？赋值
                                 preparedStatement.setObject(i + 1,o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }

                    }
                },
                new JdbcExecutionOptions.Builder().withBatchSize(2).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(QWConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
        return sinkFunction;

    }


}
