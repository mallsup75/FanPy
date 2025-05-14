select 
transaction_id
,customer_id
,PARSE_DATE('%m/%d/%Y', transaction_date) AS transaction_date  
,product_id
,quantity
,price_per_unit
,total_amount
,payment_method
,transaction_status
,transaction_source
,purchase_category
,region
, PARSE_TIMESTAMP('%m/%d/%Y %H:%M', created_at) AS created_at
, PARSE_TIMESTAMP('%m/%d/%Y %H:%M', updated_at) AS updated_at

   from {{ source('copeland_seeds', 'copeland_transactionaldata') }}   
