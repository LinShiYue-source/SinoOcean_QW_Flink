package sinoocean.qw.bean;

import lombok.Data;

/**
 * Date : 2022-04-14 14:40:07
 * Description :
 */
@Data
public class UserAction {
    String id;
    String userName;
    Integer department_id;
    Integer gender;
    Integer action_id;
    Long actionTime;
}
