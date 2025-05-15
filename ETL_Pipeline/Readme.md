transformation tool of choice: dbt


data platform of choice: Big Query (Snowflake considerations are included)


data platform disclaimer: This solution uses Google BigQuery as the data platform instead of Snowflake, as specified in the exercise. 
I chose BigQuery because my hands-on dbt environment is configured to connect to BigQuery by default, aligning with the dbt 1.9 project
setup used throughout this exercise (e.g., hale-equator-455714-m5.dbt_mallsup.copeland_wide).


While BigQuery and Snowflake differ in architecture, querying syntax, and performance characteristics, 
the core functionality of the solution—such as JSON parsing, incremental materialization, and MDM schema design—remains equivalent.
Key differences, including BigQuery’s use of JSON_EXTRACT_SCALAR versus Snowflake’s GET_PATH, and BigQuery’s serverless architecture
versus Snowflake’s compute-storage separation, have been considered to ensure compatibility. 

When it comes to timestamp handling, TIMESTAMP would likely be TIMESTAMP_NTZ in Snowflake, and Snowflake's TO_TIMESTAMP would handle parsing.

The solution leverages BigQuery’s strengths, such as native JSON support and seamless dbt integration, to achieve the exercise’s objectives effectively.



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




How to schedule on daily basis: ADF orchestration pipeline with dbt pipeline integration:

![Alt text](https://github.com/mallsup75/FanPy/blob/cope/ETL_Pipeline/pipeline.JPG)

![Alt text](https://github.com/mallsup75/FanPy/blob/cope/ETL_Pipeline/dbt-etl-pipeline.JPG)

![Alt text](https://github.com/mallsup75/FanPy/blob/cope/ETL_Pipeline/erd-pipelinedesign.JPG)
