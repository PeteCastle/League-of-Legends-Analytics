# League of Legends Data Analytics
*DataTalks.Club's Data Engineering Zoomcamp Project*
![League of Legends Data Analytics Logo](resources/images/2023-04-26-17-27-23.png)

## Project Overview
The League of Legends Analytics Website is a software-based project aimed at providing users with current trends in gameplay such as champion picks, bans, and other important statistics to help them make informed decisions about their gameplay strategies improve their skills. The project is designed to help both casual and competitive players, offering valuable insights that can benefit players at all skill levels. For example, casual players can use the data to experiment with new champions and strategies, while competitive players can use the insights to optimize their gameplay and stay ahead of the competition.

![League of Legends Data Analytics Architectural Diagram](resources/images/2023-04-26-17-58-10.png)

The project uses an extract, transform, and load (ETL) model and the League of Legends public API to obtain player data, including match results and league details.
To ensure efficient workflow orchestration, the project utilizes different tools such as dbt and Prefect. For cloud storage and programming, Azure services including Blob Storage and Databricks are utilized. All extracted data will be analyzed using PowerBi and the results will be made publicly available to users via the website.

**Disclaimer:**
*League of Legends Analytics is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties. Riot Games and all associated properties are trademarks or registered trademarks of Riot Games, Inc*

## Dataset
### [League of Legends Public API](https://developer.riotgames.com/apis)
The League of Legends Public API is a freely accessible interface that provides access to various data related to the popular online multiplayer game League of Legends. This API can be used to retrieve information on a wide range of topics related to League of Legends, including game statistics, player profiles, match histories, and more. The data is returned in JSON format, making it easy to work with programmatically. Some examples of the types of data that can be retrieved from the League of Legends Public API include player rankings, champion statistics, and match details.

### [Data Dragon](https://developer.riotgames.com/docs/lol)
Data Dragon is a resource provided by Riot Games, the developer of League of Legends, that contains all the game's static data. This includes data on champions, items, maps, and other game assets that do not change during gameplay. Data Dragon is available in the form of compressed JSON files, making it easy to download and process. The data contained within Data Dragon can be used for a wide range of purposes, such as building League of Legends-related applications or performing data analysis. Some examples of the types of data that can be found in Data Dragon include champion abilities, item attributes, and map layouts. <br> 
Not all static files can be found on the data dragon.  Some assets are obtained from [this GitHub repository](https://github.com/InFinity54/LoL_DDragon)

## Technologies/Tools Used
1. **Prefect** (*both local and cloud*) <br>
Prefect, an open-source workflow automation tool, was used to build, schedule, and monitor the **ETL pipelines**. With Prefect, it was possible to create and manage complex workflows that involved multiple steps and dependencies. Prefect workflows could be run on the local machine or deployed to the cloud using Prefect Cloud, which provided additional features such as a centralized dashboard for monitoring the workflows, automatic error handling, and the ability to scale the workflows dynamically.

2. **Azure Cloud** <br>
Azure Cloud is a **cloud computing platform** provided by Microsoft that offers a wide range of services and tools for building, deploying, and managing applications and services in the cloud. Azure Cloud can be compared to Google Cloud Platform, which is a cloud computing platform provided by Google.

3. **Azure Container Instance** <br>
Azure Container Instance is a service that allows you to **run Prefect deployments** as Docker containers in the cloud without having to manage virtual machines or orchestration services. Azure Container Instance can be compared to Cloud Run, a fully managed serverless container platform provided by Google.

4. **Azure Blob Storage** *and Data Lake Storage Gen 2* <br>
Azure Blob Storage is a scalable and highly available object storage service that allows you to **store and retrieve** large amounts of unstructured **data**, such as JSON files. Azure Blob Storage can be compared to Google Cloud Storage, which is a scalable and highly available object storage service provided by Google.

5. **Azure Databricks** <br>
Azure Databricks, a fast, easy, and collaborative Apache Spark-based analytics platform, was used for **data warehousing** with Spark 13.0 as the compute engine. With Azure Databricks, it was possible to easily process and analyze large amounts of data, build machine learning models, and create interactive dashboards. Azure Databricks can be compared to Google Big Query, a fully managed, petabyte-scale, and serverless data warehouse provided by Google.

6. **dbt Cloud** <br>
dbt Cloud, a cloud-based data transformation tool that allows **transformation** and management **of data using SQL**, was used for data warehousing solution.

7. **Terraform** <br>
Terraform, an open-source infrastructure as code (IAC) tool, was used to provision and **manage Azure Cloud resources** in a repeatable and automated way.

8. **Power BI** <br>
Power BI, a business analytics service provided by Microsoft, was used for data analytics to visualize and analyze data from a wide range of sources. Interactive dashboards and visualizations were created to gain insights from League of Legends data.   Power BI can be compared to Google Data Studio, a free and easy-to-use business intelligence and data visualization platform provided by Google.

## Project Directory Details
| File or Directory                        | Description                                                                                                                                                                                                                         |
|------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| configs/                                 | A directory containing all non-sensitive settings in the project.                                                                                                                                                                   |
| configs/dataframe_config.json            | A JSON file listing all relevant, redundant, and irrelevant columns obtained during the ETL process.  I suggest not to modify this JSON to prevent errors in upstream tasks.                                                        |
| configs/flows_config.json                | A JSON file listing all settings related to League of Legends.                                                                                                                                                                      |
| credentials/credentials.json             | Sensitive settings that provides secure access to all cloud services.  Contains the following keys.                                                                                                                                 |
| databricks/create_tables_from_lake.ipynb | A notebook file which allows the importation of CSV (or parquet files in older versions) to the Data Warehouse using PySpark API.                                                                                                   |
| dbt/                                     | Project folder used by dbt cloud.                                                                                                                                                                                                   |
| dbt/macros/*.sql                         | All custom macros used by SQL models. Reusable code snippets that can be used to create custom SQL functions or perform common transformations on data.                                                                             |
| dbt/models/core/*.sql                    | All models used for data analytics. Defines a specific table or view in a data warehouse. A dbt model is typically used to transform, aggregate, or join data from different sources to create a single, coherent view of the data. |
| dbt/models/core/schema.yml               | Contains all schema, tests, and column definitions for all core models *TBA*.  Also includes source modes outside of dbt.                                                                                                           |
| dbt/models/staging/*.sql                 | All views (intermediate step) in terms of select columns as a succeeding step of loading data from Spark.                                                                                                                           |
| dbt/models/staging/schema.yml            | Contains all schema, tests, and column definitions for staging tables.                                                                                                                                                              |
| flows/                                   | All flows used by Prefect during ETL from league API, basic data transformation, and upload to data lake.                                                                                                                           |
| flows/api_ingestion/api_ingest.ipynb     | DEPRECATED.  Most likely to be removed soon.                                                                                                                                                                                        |
| flows/dependencies/api_requests.py       | Python file responsible for HTTP requests; handling concurrent requests; and API method extraction.                                                                                                                                 |
| flows/dependencies/dataframe_extras.py   | Python file for increased abstraction on certain Dataframe transformation.                                                                                                                                                          |
| flows/dependencies/league_entries.py     | Python file responsible for transforming JSON file obtained from Ranked Leagues API Method to flattened DataFrame.                                                                                                                  |
| flows/dependencies/match_entries.py      | Python file responsible for transforming JSON file obtained from Matches API Method to flattened DataFrame.                                                                                                                         |
| flows/dependencies/method_ingestion.py   | Python file as a method ingestion manager for Python files above.                                                                                                                                                                   |
| flows/all_flows.py                       | Python file as an-easy-to-use ETL flow for leagues, matches, player info, etc.  Has to methods:  ETL for all leagues, and ETL for each leagues.                                                                                     |
| flows/convert_parquet_to_csv.py          | Python file to convert all Parquet files to CSV.  This is due to the inability of Spark to convert Parquet columns when multiple files are read.                                                                                    |
| flows/league_assets.py                   | Python file as an ETL flow for LOL Data Dragon.                                                                                                                                                                                     |
| powerbi/main.pbix                        | The only data analytics file included in the project.                                                                                                                                                                               |
| resources/                               | Default save location for external resources                                                                                                                                                                                        |
| resources/images                         | Images only used for documentation purposes.                                                                                                                                                                                        |
| resources/datasets                       | Where all local tables, CSVs, or Parquet files are saved.                                                                                                                                                                           |
| terraform/                               | Terraform's project directory                                                                                                                                                                                                       |
| terraform/main.tf                        | Where all Azure Cloud services are managed as a code.                                                                                                                                                                               |
| *-depoyment.yaml                         | Deployment files created by Prefect.                                                                                                                                                                                                |
| *.md                                     | Contains all official documentation about this project.                                                                                                                                                                             |


# Reproduction Step
## Setting Up

### Clone the repository
`git clone https://github.com/PeteCastle 2023-Data-Engineering-Zoomcamp-Project`

### Installing Packages
`pip install -r requirements.txt` <br>
*The list may be incomplete as I've manually searched for the packages.*

### Application Installation
* Install Terraform [here](https://developer.hashicorp.com/terraform/downloads)
* Install PowerBi [here](https://powerbi.microsoft.com/en-us/downloads/)
* Insta Azure CLI [here](https://learn.microsoft.com/en-us/cli/azure/).  This is a required dependency by Terraform

### Configuring your Microsoft Azure Services
1. Go to the Azure website at https://azure.microsoft.com and click the "Start free" button at the top of the page.
2. Sign in or create a Microsoft account.
3. Once you've completed the sign-up process, you will be taken to the Azure portal dashboard or click [this link](https://portal.azure.com/#home)
4. From the dashboard, search for "Subscriptions" from the search bar.  A subscription is a logical container used to provision resources in Azure.  
5. Click on the "Add" button to create a new subscription. Choose the subscription type you want to use. The free subscription option will give you access to a limited set of Azure services, but will be sufficient for most small projects.
6. Now that you have a subscription, you can configure the Azure CLI. To do this, open a terminal window and enter the following command: <br>
`az login`
7. `az account set --subscription <subscription_id>`
Replace <subscription_id> with the ID of the subscription you want to use, which you can find in the Azure portal under "Subscriptions".

### Provisioning your Cloud Services
1. From your CLI, `cd <YOUR_PROJECT_DIRECTORY>`
2. `cd ./terraform`
3. `terraform init`
4. `terraform plan`
5. `terraform apply`

**IMPORTANT NOTE:** Some service names have a global uniqueness constraint (such as Azure Storage Account).  If you encounter an error, you have to change the name of the service.  

### Obtaining your Riot API key
Riot API keys are needed to run this pipeline.  You can obtain your API key by following these steps:
1. Go to https://developer.riotgames.com/ and create an account. You can use your Riot account if you play League of Legends, Valorant, or any other Riot game.
2. Accept the terms of service and click on "Get Started" to create your API key.
![Sample Riot API Screenshot](resources/images/2023-04-26-21-23-39.png)
3. Generate your API key and save it in `./credentials/credentials.json`.  More on this later.
4. The more API keys, the merrier.  Note that the rate limits are 20 requests every 1 seconds(s) and 100 requests every 2 minutes(s). 
5. You have to refresh the API every 24 hours if you have Production API (default).

### Setting Up Prefect Cloud
1. Go to https://cloud.prefect.io/ and create an account.
2. Create a workspace of your choice.
3. Go to your CLI, and then enter `prefect cloud login`
4. You may either login in with a web browser (*easier*), or paste an API key.
5. Authorize the application
6. You'll be notified if the login is successful.
7. Go back to the webpage.  From the bottom left, open your account.  Then beside your profie icon, click on "Workspace Settings".
8. You'll see the command to set your workspace.  Copy the code and paste it in your CLI.
9.  Now all your flows are now run in the cloud.  You can check the status of your flows in the Prefect Cloud website.
Note: `prefect orion start` won't do anything as all flows are automatically run in the cloud, not locally.
  
### Configuring your credentials
Credentials are needed to run the entire pipeline.  You credentials file are stored in `./credentias/credentias.json`.  The following is the sample structure of this JSON file.  The steps on obtaining them is presented after the JSON structure.

```
{
    "API_KEYS" : [
        "RGAPI-abcd123456-random-letters-and-numbers-0",
        "RGAPI-abcd123456-random-letters-and-numbers-1",
        "RGAPI-abcd123456-random-letters-and-numbers-2",
        "RGAPI-abcd123456-random-letters-and-numbers-3",
    ],
    "AZURE_BLOB_STORAGE_CREDENTIALS" : "DefaultEndpointsProtocol=https;AccountName={YOUR_ACCOUNT_NAME};AccountKey={YOUR_ACCOUNT_KEY};EndpointSuffix=core.windows.net",
    "AZURE_BLOB_STORAGE_KEY" : "{YOUR_ACCOUNT_KEY}",
    "AZURE_BLOB_CONTAINER" : "{STORAGE_CONTAINER_NAME}",
    "AZURE_BLOB_PROJECT_NAME": "{STORAGE_NAME}",
    "PREFECT_API_KEY" : "pnu_random_letters_and_numbers",
    "PREFECT_API_URL" : "https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}",
    "DATABRICKS_ACCESS_TOKEN" : "dapia1234randomnumbers",
    "SPARK_MASTER_HOSTNAME" : "https://adb-randomnumbers.69.azuredatabricks.net",
    "SPARK_MASTER_PORT" : "443",
    "SPARK_MASTER_HTTP_PATH" : "sql/protocolv1/o/this-is-databricks-id/this-is-the-custer-id",
    "DATABRICKS_CLUSTER_ID" : "random-letters-and-numbers"
}
```

#### Riot API Keys 
`API_KEYS` <br>
Already stated in [Obtaining your Riot API key](#obtaining-your-riot-api-key)
#### Azure Blob Storage
From Azure Portal, search for `Storage Accounts`
![](resources/images/2023-04-26-21-30-30.png)
Cick the storage account of your choice
![](resources/images/2023-04-26-21-31-42.png)
A window will pop up.  Click on `Access Keys`
![](resources/images/2023-04-26-21-34-45.png)

`AZURE_BLOB_STORAGE_CREDENTIALS` <br>
Your Blob Storage Credentials is the `Connection string` as shown in the image above.

`AZURE_BLOB_STORAGE_KEY` <br>
Your Storage Key is the `Key` attribute.

Now navigate to `Containers`
![](resources/images/2023-04-26-21-38-33.png)

`AZURE_BLOB_CONTAINER` <br>
Your Blob Container is one of the `Name` attribute.  Based from the screenshot, it would be `samplecontainer`

`AZURE_BLOB_PROJECT_NAME` <br>
Your Blob Project Name is the in the header.  Based from the screenshot, it would be `thisprojectsample`

#### Prefect
`PREFECT_API_KEY` <br>
Your prefect API key is found in the Prefect Cloud website.  On the bottom left of the screen, click on your profile icon.  Then click on the Settings icon beside your name.  Select `API Keys`.  Create one, and copy the key.

`PREFECT_API_URL` <br>
1.  Go to your workspace.
2.  Notice the URL.  It would be something like `https://app.prefect.cloud/account/YOUR_ACCOUNT_ID/workspace/YOUR_WORKSPACE_ID/flow-runs`
3.  Your `PREFECT_API_URL` will be `https://api.prefect.cloud/api/accounts/YOUR_ACCOUNT_ID/workspaces/YOUR_WORKSPACE_ID/`
  
#### Databricks/Spark
From Azure Portal, search for `Azure Databricks` and then select the service you've created.  Launch the Workspace from the Overview page.
![](resources/images/2023-04-26-22-00-07.png)

At the upper right side of the screen, click on your username -> User Settings 

`DATABRICKS_ACCESS_TOKEN`<br>
Your Access Token is found in the `Access Tokens` tab.  Click on `Generate New Token` and copy the token.
![](resources/images/2023-04-26-22-01-51.png)

At the left side of the screen, hover then click on `Compute`.  Select the cluster you've created.  
![](resources/images/2023-04-26-22-04-18.png)

Click on `Advanced Options` then `JDBC/ODBC`

`SPARK_MASTER_HOSTNAME` <br>
Your Spark Master Hostname is the `Server Hostname` Attribute

`SPARK_MASTER_PORT` <br>
Your Spark Master Port is the `Port` Attribute

`SPARK_MASTER_HTTP_PATH` <br>
Your Spark Master HTTP Path is the `HTTP Path` Attribute

`DATABRICKS_CLUSTER_ID` <br>
Your Databricks Cluster ID is the series of numbers and letters after the backlash in the `HTTP Path` Attribute.
If your HTTP path is `sql/protocolv1/o/123456789/1244-678999-abcdef`, then the cluster ID is `1244-678999-abcdef`

## Reproducing the entire ETL Pipeline


### How does the Prefect ETL process work?
The following is a simplified process on how ETL works from ingestion from API to storage in Azure Blob Storage:
1. 

### Deploying `all_flows:etl_per_league` to Prefect
`cd {YOUR_PROJECT_DIR}`

`prefect deployment build ./flows/all_flows.py:etl_per_league --name="etl_all"`

(Optional) You may enter the following parameters in `etl_all-deployment.yaml` file:
```
parameters: {
    "queue": "RANKED_SOLO_5x5",
    "tier" : "challengerleagues",
    "division" : "I",
    "pages" : 1,
    "regions" : ["br1","eun1","euw1","jp1","kr","la1","la2","na1","oc1","tr1","ru","ph2","sg2","th2","tw2","vn2"],
    "ACCOUNT_INPUT_LIMIT" : 10000,
    "MATCH_INPUT_LIMIT" : 10000,
    "API_KEYS" : ["ENTER_YOUR_API_KEY_HERE remove this if you have api key set in credentials.json"], 
    "MAXIMUM_CONCURRENT_REQUESTS" : 500,
  }
```
Enter your API keys here OR in `./credentials/credentials.json' file.  The field is a list, so you can add more API keys if you want to hasten your API requests.  
Note, I HIGHLY suggest entering a field in ACCOUNT_ONPUT_LIMITS and MATCH_INPUT_LIMITS.  If you don't the program will query all accounts or matches that it receives from the API. For context: An API call from a league returns ~4000 accounts, and a match call from each account returns ~10 match by default.

`prefect deployment apply "etl_all-deployment.yaml"`

You should see this in your Prefect local UI
![](resources/images/2023-03-29-16-08-17.png)

Don't forget to add an agent: `prefect agent start default`

### Deploying `all_flows:all_league` to Prefect

`prefect deployment build ./flows/all_flows.py:etl_all_league --name="etl_all"`

Same parameters as above, but without the `tier` attribute since the program will ETL all tiers.

### Deploying `league_assets:get_league_assets` to Prefect
`prefect deployment build ./flows/league_assets.py:get_league_assets --name="get_league_assets" --apply`
No need for arguments since this is a static ETL.  This can be run once per League patch.
