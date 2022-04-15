package sinoocean.qw.bean;

import lombok.Data;

/**
 * Date : 2022-04-12 15:12:39
 * Description :
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;

}
