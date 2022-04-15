package sinoocean.qw.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.mapred.Merger;

/**
 * Date : 2022-04-15 10:45:26
 * Description :
 */
@Data
@AllArgsConstructor
public class UserWide {
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

    public UserWide(UserLogin userLogin, UserAction userAction) {
        mergerUserlogin(userLogin);
        MergerUserAction(userAction);
    }

    private void MergerUserAction(UserAction userAction) {
        this.id = userAction.id;
        this.userName = userAction.userName;
        this.department_id = userAction.department_id;
        this.gender = userAction.gender;
        this.action_id = userAction.action_id;
        this.actionTime = userAction.actionTime;
    }

    private void mergerUserlogin(UserLogin userLogin) {
        this.loginTime = userLogin.loginTime;
    }


}
