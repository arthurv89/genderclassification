select id as user_id, b.customer_number account_number, gender
from liverw.dps_user a, liverw.bol_user b
where 
a.id = b.user_id
and
rownum <= 5000;
