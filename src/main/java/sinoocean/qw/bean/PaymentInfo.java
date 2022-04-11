package sinoocean.qw.bean;
import lombok.Data;
import java.math.BigDecimal;

/**
 * Date : 2022-04-11 10:33:31
 * Description :
 */
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;

}
