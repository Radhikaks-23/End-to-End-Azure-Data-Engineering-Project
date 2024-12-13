## **Part 2: Data Transformation**

Data is Loaded into Azure Databricks where can create PySpark Notebooks. Cluster nodes, and compute automatically managed by the Databricks service. The Initial Data is cleaned and processed in two steps. Bronze to Silver and Silver to Gold. 0. Mounting the ADLS

1) In Bronze to Silver transformation, we apply Attribute Type Changes and move this preprocessed data from Bronze to Silver folders.
2) In Silver to Gold transformation, we rename the Attributes to follow similar Naming Convention throughout the database. Then we move this into Gold folder.
3) These Notebooks are integrated into the Azure Data Factory Pipeline. Thus automating the Data Ingestion and Transformation process.

![Data Transformation](https://github.com/user-attachments/assets/0c2d8485-a131-4427-ab39-b772b545783c)

## **Part 3: Data Loading**
Load the "gold" level data and run the Azure Synapse Pipeline. This pipeline:

Retrieves the Table Names from the gold folder.

For each table, A Stored Procedure is executed which creates and updates View in Azure SQL Database..

![image](https://github.com/user-attachments/assets/5f570d63-80ab-417b-86ea-5f5e723bdf06)

![image](https://github.com/user-attachments/assets/25ef91a3-907b-44b4-bf62-51283bdab37b)

