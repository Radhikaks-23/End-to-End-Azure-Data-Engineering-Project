## **Part 2: Data Transformation**
Data is Loaded into Azure Databricks where can create PySpark Notebooks. Cluster nodes, and compute automatically managed by the Databricks service. The Initial Data is cleaned and processed in two steps. Bronze to Silver and Silver to Gold. 0. Mounting the ADLS

In Bronze to Silver transformation, we apply Attribute Type Changes and move this preprocessed data from Bronze to Silver folders.
In Silver to Gold transformation, we rename the Attributes to follow similar Naming Convention throughout the database. Then we move this into Gold folder.
