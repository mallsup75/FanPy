Your company is implementing MDM using a central hub (e.g. Informatica MDM or Profisee ) and needs to integrate 

1. customer CRM

2. customer ERP

3. customer eCommerce


Tasks:


1. design of data flow for integration into hub

   
'MDM_Integration/MDM_DDLs/Hub_tables.sql'


....create tables: 'Source_System' , 'Source_Customer_Record' , 'Record_Mapping' , 'Golden_Record'



'MDM_Integration/SQL/Insert_statements.sql'


....insert sample provided records from csv to hub tables.



'MDM_Integration/SQL/Retrieve_query.sql'


....sample retrieval query from Golden_Record with joins


'MDM_Integration/SQL/Output.txt'


....sample retrieval results from Golden_Record
   
![Alt text](https://github.com/mallsup75/FanPy/blob/cope/MDM_Integration/MDM_DDLs/profisee-hub.JPG)


![Alt text](https://github.com/mallsup75/FanPy/blob/cope/MDM_Integration/MDM_DDLs/erd-golden-record-mapping.JPG)


3. explain how to ensure data governance and quality

A: Profisee governs the actual content of master data (e.g. correct customer names) ; profisee for cleansing 
and deduplication, rules handling, and stewardship interaction

As for other tenants of governance:  Purview catalogs, classifies, and tracks where and how that data is used across the enterprise.

4. how to handle versioning, historical tracking, and synchronization between hub and operational systems

A: Best practice is to rely on MDM Hub provided GUI to handle versioning, historical tracking, and synchronization. 
This is functionality that is better off buying from a vendor vs. building yourself.
