# Azure End to End Data Engineering Pipeline

This project builds an End-to-End Azure Data Engineering Solution. A Pipeline performing Data Ingestion, ETL and Analytics all-in-one solution using Microsoft Azure Services and Power BI.

## Goal of the Project

The goal is to create an Azure solution which can take an On-premise Database such as the Microsoft SQL Server Management System (SSMS) and move it to the Cloud. It does so by building an ETL pipeline using Azure Data Factory, Azure Databricks and Azure Synapse Analytics.

This solution can be connected to a visualization and reporting dashboard using Microsoft Power BI.
![image](https://github.com/user-attachments/assets/6557dd5f-0b41-4613-af46-09d7813b8ffc)

Data Migration to the Cloud is one of the most common scenarios the Data Engineers encounter when building solutions for a small-medium organization.
By working on this project, I was able to learn these skills:

* Data Ingestion
* ETL techniques using Azure Cloud Services
* Data Transformation
* Data Analytics and Dashboard Reporting
* Data Security and Governance


## Prerequisites:

1) Microsoft SQL Server Managment System (SSMS)
2) Azure Subscription (Azure Data Lake Storage Gen2, Azure Data Factory, Azure Key Vault, Azure Databricks, Azure Synapse Analytics, Microsoft Entra ID)
3) Microsoft Power BI
4) Set up "AdventureWorksLT2017" Database with credentials . Set up the same credentials as Secrets in Azure Key Vault

The Database used for this project demonstration is:
AdventureWorksLT2017 Sales Database

## End Notes

* This project provides a great overview to many of Azure services such as Azure Data Factory, Azure Databricks, Azure Synapse Analytics.

* The resources we create in Azure can be secured by adding contributors into a Security Group. This feature is offered in Microsoft Entra ID (previously Azure Active Directory).
Thus, whoever belonging to the Security group can access and contribute to the project freely.

* The Database is although small, made it easier to visualize the scope and working of various services at once.

* Another thing to note is that, project couldve been made smaller using only Azure Data factory but I added Databricks and Synapse Analytics to explore what these have to offer.


