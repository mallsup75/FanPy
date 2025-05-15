Assumptions:


1. 'Eventuhub_id' is unique PK; noting gaps in the value sequencing, which is a potential warning sign for reliability of this value as PK.


2. 'Deviceid' is a foreign key referencing a Device table; and is worthy of having a dimension of it's own, even though it presents 1:1 with 'Eventhub_id' in the sample


'DeviceType' is categorical information about the device.


'Logged' is a timestamp indicating when the event was logged; format needs to be consistent and parsed into a proper timestamp data type; assume this is the "updated_at" value for the record; risky due to logged date sequence <>  eventuhub_id sequence.


3. For 'Alarms' field, parse out the alarm 'id' from 'time', so that querying for specific alarms is easy, as-is working with the alarm time.

 
4. Time elapsed between 'Alarm' and 'Logging' is meaningful. This is stored as 'Alarm_to_Logging_Minutes'


5. Companies: JSON array of company IDs represents a 1:N relationship between Event and Companies; flattening into seperate rows in a linking table is appropriate to adhere to relational modeling principles.

  
6. Column headers or definitions are not available; would certainly inquire!


7. 'P7' appears as an unknown datetime value; with more context could be another use case for "minutes elapsed" type logic


8. None of the values are geographic co-ordinates (lat and long)


9. 'P9' is a pre-normalized value for ML, due to value range 0-1


10. Exact Decimal precision is needing to be retained throughout the model; i.e. simplifying to .123 or .1234 decimal places results in a loss of meaningful detail


11. Alarm values ranging only from [A1, A2, A3]  are representative of all possible values; therefore adding in Cross-join strategies for soft-coding is not good practice here due to compute implied by cross-joins in pipeline processes.


P1-P10: some data types seem inconsistent. "OFF" and "ON" suggest boolean or categorical data. "nan" should be considered for update instead to NULL.


3. Create a Fact Table: The core event information (EVENTUHUB_ID, DEVICEID, LOGGED, parsed ALARMS, linking to COMPANIES, and the cleaned/understood "P" parameters) would reside in a fact table.
4. Standardize Data Types: Explicitly cast columns to appropriate data types (integers, floats, timestamps, booleans, strings).
5. Handle Missing Values: Implement a consistent strategy for representing and handling missing data.
6. Rename Columns: Provide clear and business-relevant names for all columns, especially P1 - P10.




TO DO:

Singular test for other alarm values!
Relationship check!


![Alt text](https://github.com/mallsup75/FanPy/blob/cope/DataModeling/Staging/copeland/wide-event-dag2.JPG)


![Alt text](https://github.com/mallsup75/FanPy/blob/cope/DataModeling/erd-data-modeling22.JPG)
