```sql
use tpcds_sf5120_withdecimal_withdate_withnulls;
--q4.sql--

WITH year_total AS (
 SELECT c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
        count(1) year_total,
        's' sale_type
 FROM customer, store_sales, date_dim
 WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk
 GROUP BY c_customer_id,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_country,
          c_login,
          c_email_address,
          d_year
 UNION ALL
 SELECT c_customer_id customer_id,
        c_first_name customer_first_name,
        c_last_name customer_last_name,
        c_preferred_cust_flag customer_preferred_cust_flag,
        c_birth_country customer_birth_country,
        c_login customer_login,
        c_email_address customer_email_address,
        d_year dyear,
         count(1) year_total,
        'c' sale_type
 FROM customer, catalog_sales, date_dim
 WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk
 GROUP BY c_customer_id,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_country,
          c_login,
          c_email_address,
          d_year
 UNION ALL
 SELECT c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,count(1) year_total
       ,'w' sale_type
 FROM customer, web_sales, date_dim
 WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk
 GROUP BY c_customer_id,
          c_first_name,
          c_last_name,
          c_preferred_cust_flag,
          c_birth_country,
          c_login,
          c_email_address,
          d_year)
 SELECT
   t_s_secyear.customer_id,
   t_s_secyear.customer_first_name,
   t_s_secyear.customer_last_name,
   t_s_secyear.customer_preferred_cust_flag
 FROM year_total t_s_firstyear,year_total t_s_secyear
 WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id
   and t_s_secyear.sale_type = 's'
   and t_s_secyear.dyear = 2001+1
   and t_s_firstyear.sale_type = 's'
   and t_s_firstyear.dyear = 2001
   and t_s_firstyear.year_total > 0
 ORDER BY
   t_s_secyear.customer_id,
   t_s_secyear.customer_first_name,
   t_s_secyear.customer_last_name,
   t_s_secyear.customer_preferred_cust_flag
 LIMIT 100
```            
`count(1) year_total`
`sum((((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2) ) year_total`