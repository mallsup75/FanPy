{{
    config(
        materialized='view',
        schema='copeland_schema'
        
    )
}}


select 
count(transaction_id) transaction_count
,count(customer_id) customer_count
,extract(month from transaction_date) transaction_month
,extract(year from transaction_date) transaction_year
,count(product_id) product_count
,sum(quantity) sum_quantity
,sum(total_amount) sum_total_amount
##,payment_method
,transaction_status
##,transaction_source
,max(created_at) last_created_at
,max(updated_at) last_updated_at

from 

{{ ref('transaction_fact')}}

where transaction_status = 'Completed'

group by 3,4,8
