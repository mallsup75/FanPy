transformation tool of choice: dbt


data platform of choice: Big Query (Snowflake considerations are included)


Steps:


1.Created/Uploaded 'TransactionalData.csv' as Seed file in dbt 


2.create source schema.yml: 'src_copeland.yml' for purpose of assigning schema, defining of seeds


3.create stage table from source: 'stg_copeland_transactions.sql'


4..convert STRINGS to TIME


5.create snapshot.yml: 'snapshots.yml'



6.create snapshots from stage: 'snapshot_customer.sql' , 'snapshot_product.sql'



assumptions: 


1.Transaction_id is unique identifier for transaction


2.customer_id is unique identifer for customer, and customers will have multiple transactions over time; customer should be snapshot to enable SCD 2


3.transaction_date is sufficient as a date; don't expect to make this a DATETIME in future


4.product_id is the unique identifier for product, product should be snapshot to enable SCD 2


5.quantity is always a whole number


6.price_per_unit , total_amount; no currency conversions are needed


7.payment_method, transaction_status, transaction_source are attributes of transaction (not customer or product )


8.purchase_category is an attribute of product


9.region is an attribute of customer


10. updated_at should be DATETIME , safe to use for pinning of snapshot/incremental logic

    

TO DO: 

Schema model looks like:


Data aggregration and cleansing (missing values, normalizing data):


Implement incremental loading for performance optimization:


How to schedule on daily basis:
![Alt text](https://github.com/mallsup75/FanPy/blob/cope/ETL_Pipeline/dbt-etl-pipeline.JPG)

![Alt text](https://github.com/mallsup75/FanPy/blob/cope/ETL_Pipeline/erd-pipelinedesign.JPG)
