select customer_id_dmd_s,global_id
from LSM_ORDERS a, LSM_ORDER_ITEMS b
where a.lor_id = b.lor_id and date_placed_dmd between sysdate - 365 and sysdate - 335
and customer_id_dmd_s in
(
select distinct customer_id_dmd_s
from lsm_orders 
where date_placed_dmd between sysdate - 365 and sysdate
and customer_id_dmd_s is not null
and rownum <= 500
)
