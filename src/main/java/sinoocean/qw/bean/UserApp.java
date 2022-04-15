package sinoocean.qw.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Date : 2022-04-15 15:05:24
 * Description :
 */
@Data
@AllArgsConstructor
public class UserApp {
    //事实表字段
    String id;
    String userName;
    Integer department_id;
    Integer gender;
    Integer action_id;
    Long actionTime;
    Long loginTime;
    //维度表字段
    String actionName;
    String departmentName;
    //来电次数
    Long dianCount;
    //来访次数
    Long fangCout;

}
