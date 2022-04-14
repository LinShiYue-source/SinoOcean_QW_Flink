package sinoocean.qw.bean;

import lombok.Data;

import java.util.Date;

/**
 * Date : 2022-04-14 11:23:58
 * Description :
 */
@Data
public class UserLogin {
    Integer id;
    String userName;
    Integer departmentId;
    Integer gender;
    Long loginTime;

}
