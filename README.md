# FanPy
End-to-End Data Engineering Components using python 

DAG Setup: Designed an Apache Airflow DAG to automate the process
![Alt_text](https://github.com/mallsup75/FanPy/blob/main/python/img/py-players-to-blob-dag.JPG)

Extract raw JSON roster data from API. landing to bronze container using orchestrated workflow.
[View my Python script](https://github.com/mallsup75/FanPy/blob/main/python/dags/fetch_team_rosters_dag.py)

Tasks: List Blobs: Download and Parse: Fetch JSONs using BlobServiceClient, parse into a Pandas DataFrame.
![Alt_text](https://github.com/mallsup75/FanPy/blob/main/python/img/py-flattenvia-extend-loop.JPG)

Tranform blobs to csv. Merge Csv datasources, merge files, normalize metrics, engineer features via pandas.
[View my Python script](https://github.com/mallsup75/FanPy/blob/main/python/dags/combine_rosters_to_silver_dag.py)

Link to PDF w/ design diagram and walk through: 
![Alt_text](https://github.com/mallsup75/FanPy/blob/main/fanpy_arch_diag.JPG)
[View the PDF](https://github.com/mallsup75/FanPy/blob/main/fanpy_datapipeline_proj.pdf)
 
The value score approach is a composite index combining salary and ADP where lower values of both are ideal (e.g. a “value” metric). 
[View my Python script](https://github.com/mallsup75/FanPy/blob/main/python/normalizeADPSalary.ipynb)

The ADP ,average draft position, is sourced  from the fantrax API.
[View my Python script](https://github.com/mallsup75/FanPy/blob/main/python/dags/players_to_blob_dag.py)

ADP is a moving number, and was captured prior to spring training and the ADP is the average across all fantrax tenants as of March 1 2025. 
 
The salary is specific to the Bestballer 150 League, and represents the bid amount/injured pickup value limited to the 12 league managers in our fantrax tenant. 

ADP was normalized , by position, with a 0-1 scale 

Salary was normalized, by position, with a 0-1 scale 

Z-Scores were calculated to standardize them for the purpose of the index.
![Alt text](https://github.com/mallsup75/FanPy/blob/main/python/img/z-scoring.JPG)

![Alt text](https://github.com/mallsup75/FanPy/blob/main/2b_normalized_impact_multiaxis_view.jpg)

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/OFD_normalized_impact_multiaxis_view.jpg)

<All data sourced from fantrax is sourced api via python using apache airflow >>

Story Told- Player Level- Tigers actively rocking with "Elly and the Tarik " Skubal !! 

At a player level, Elly De La Cruz is the best keeper in the league. With a $4M salary and 5.48 ADP, that propels Elly to the top for 2025!  With an ADP of 7, Gunnar Henderson is being drafted right behind him. The salary of $8M lands Gunnar as the #13 keeper value of the league.
 
Tigers Rock! Has the best keepers, and you can see how the rest shakes out.  

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/top_value_zone.jpg)

The worst:
![Alt_text](https://github.com/mallsup75/FanPy/blob/main/worst_value_zone.jpg)

Value Zone samples:
![Alt_text](https://github.com/mallsup75/FanPy/blob/main/1b_value_zone.jpg)

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/P_value_zone.jpg)

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/SS_value_zone.jpg)

Top 100 ADP Sampling. The difference between a Keeper and a perennial free agent. Displayed is salary trend by player and franchise.

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/spark_overyear_sample.jpg)

![Alt_text]https://github.com/mallsup75/FanPy/blob/main/tale_of_keeper_or_not.jpg)

Top 150 ADP Head to Head.
How do top players stack up, head to head, for two pairs of rosters?


![Alt_text](https://github.com/mallsup75/FanPy/blob/main/ham_v_steamers.jpg)

![Alt_text](https://github.com/mallsup75/FanPy/blob/main/snow_v_bluberry.jpg)

Top 100 ADP Sampling.
Worst drops/trades/not kept of fantrax area?

Get Brute 2021 Witt $1M (see also: Mirriam Aaaron Inc)
HH 2023 C.Walker $4M (see also:Serverless Concepts)
GRP 2021 L.Webb $3M (see also: Jims Deserts Rats)
ACC 2022 B.Reynolds $1M (see also: ghost casper)
MB 2022 M.King $3M
HAM 2022 G.Henderson $1M (trade for Luis Castillo)
EXC 2021 L.Gilbert $1M (see also: Broome Street Bombers)
TR 2022 W.Contreras $3M
SNOW 2022 Casas $1M  (see also: Tc Tuggers)


Footnotes:
 *While the lower values are what is used, note for the purpose of data visualization, I inverted the result values so that a smaller index with appear larger in the graphs.
