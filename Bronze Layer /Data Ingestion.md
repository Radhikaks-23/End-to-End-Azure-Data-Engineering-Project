##   **Part 1: Data Ingestion**

1) Restore the AdventureWorksLT2017 Database from the .bak file.

![image](https://github.com/user-attachments/assets/025561c8-c6e7-4c14-bf57-568fe6c0e118)

2) Setup the Microsoft Integration Runtime between Azure and the On-premise SQL Server.

3) Create a Copy Pipeline which loads the data from local on-premise server into Azure Data Lake Storage Gen2 "bronze" directory.

**Note that the Data is stored in "Parquet format" in ADLS Gen2 storage folders**

![Copy_activity_new](https://github.com/user-attachments/assets/34755ac9-3d1a-4264-bdc2-4a456eeb90df)


