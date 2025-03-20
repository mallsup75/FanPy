# FanPy
End-to-End Data Engineering Components Extract raw JSON roster data from bronze container using orchestrated workflow.  How: DAG Setup: Designed an Apache Airflow DAG to automate the process: Tasks: List Blobs: Query bronzeDownload and Parse: Fetch JSONs using BlobServiceClient, parse into a Pandas DataFrame.

Link to PDF w/ design diagram and walk through: https://github.com/mallsup75/FanPy/blob/main/fanpy_datapipeline_proj.pdf


Keeper Value Scoring - 150 Bestballer Baseball  3/13/25 
About: 
The value score approach is a composite index combining salary and ADP where lower values of both are ideal (e.g. a “value” metric).
 
The ADP ,average draft position, is sourced  from the fantrax.

ADP was normalized , by position, with a 0-1 scale 

Salary was normalized, by position, with a 0-1 scale 

Z-Scores were calculated to standardize them for the purpose of the index.

ADP factored with Player Salary is used to calculate 

<All data sourced from fantrax is sourced api via python using apache airflow >>

Story Told- Player Level- Tigers actively rocking with "Elly and the Tarik " Skubal !! 

At a player level, Elly De La Cruz is the best keeper in the league. With a $4M salary and 5.48 ADP, that propels Elly to the top for 2025!  With an ADP of 7, Gunnar Henderson is being drafted right behind him. The salary of $8M lands Gunnar as the #13 keeper value of the league.
 
Tigers Rock! Has the best keepers, and you can see how the rest shakes out.  

https://github.com/mallsup75/FanPy/blob/main/top_value_zone.jpg


Link to google doc share w/ sample outputs and insights: https://docs.google.com/document/d/1aT6LcVgrBjPe1tGGc1BZeq9taQvMbl27AsXyitxrVEk/edit?usp=sharing
