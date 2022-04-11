package sinoocean.qw.bean;
import lombok.Data;
import java.math.BigDecimal;

/**
 * Date : 2022-04-11 10:19:27
 * Description :
 */
public class OrderDetail {
    Long id;
    Long order_id ;
    Long sku_id;
    BigDecimal order_price ;
    Long sku_num ;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    Long create_ts;

}
