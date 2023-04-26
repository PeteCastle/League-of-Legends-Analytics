terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = ">=1.14.2"
    }
  }
}

provider "databricks" {
  host = azurerm_databricks_workspace.db_project.workspace_url
}

data "databricks_current_user" "me"{
  depends_on = [azurerm_databricks_workspace.db_project]
}

data "databricks_node_type" "smallest" {
  local_disk = true
  category = "General Purpose"
}

data "databricks_spark_version" "latest_version" {
  depends_on = [azurerm_databricks_workspace.db_project]
  latest = true
  # long_term_support = true
  ml = false
  gpu = false
  scala = "2.12"
  spark_version = "3.4.0"


}

locals{
  credentials = jsondecode(file("../credentials/credentials.json"))
}

provider "azurerm" {
  features{}
}

resource "azurerm_resource_group" "rg_project" {
  name     = "data-engineering-zoomcamp-project"
  location = "southeastasia"
}

resource "azurerm_storage_account" "sa_project" {
  name                     = "zoomcampsaproject"
  resource_group_name      = azurerm_resource_group.rg_project.name
  location                 = azurerm_resource_group.rg_project.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  access_tier = "Hot"
}

resource "azurerm_storage_container" "sc_project" {
  name                  = "zoomcampcontainerproject"
  storage_account_name  = azurerm_storage_account.sa_project.name
  container_access_type = "blob"
}

# resource "azurerm_storage_container" "storage_block" {
#   name                  = "zoomcamp-storage-block"
#   storage_account_name  = azurerm_storage_account.sa_project.name
#   container_access_type = "blob"
# }

resource "azurerm_databricks_workspace" "db_project" {
  name                = "zoomcampdbproject"
  resource_group_name = azurerm_resource_group.rg_project.name
  location            = azurerm_resource_group.rg_project.location
  sku                 = "standard"
  managed_resource_group_name = "zoomcampdbproject-rg"
}

resource "databricks_cluster" "cluster_project" {
  cluster_name            = "zoomcampclusterproject"
  node_type_id            = data.databricks_node_type.smallest.id
  spark_version           = data.databricks_spark_version.latest_version.id

  autotermination_minutes = 10
  num_workers = 0
  # autoscale {
  #   min_workers = 2
  #   max_workers = 4
  # }
  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.databricks.delta.preview.enabled" = "true"
    "spark.master" : "local[*]"
    "fs.azure" : "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    "fs.azure.account.key.zoomcampsaproject.blob.core.windows.net" : local.credentials["AZURE_BLOB_STORAGE_KEY"]
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}

# resource "azurerm_container_group" "zoomcamp-container-group" {
#   name                = "zoomcamp-container-group"
#   location            = azurerm_resource_group.rg_project.location
#   resource_group_name = azurerm_resource_group.rg_project.name
#   ip_address_type     = "Public"
#   os_type             = "Linux"

#   container {
#     name   = "prefect-agent-container"
#     image  = "prefecthq/prefect:2-python3.10"
#     cpu    = "1"
#     memory = "1.5"
  
#     ports {
#       port     = 443
#       protocol = "TCP"
#     }
#     secure_environment_variables = {
#       "PREFECT_API_URL" = local.credentials["PREFECT_API_URL"]
#       "PREFECT_API_KEY" = local.credentials["PREFECT_API_KEY"]
#     }
#     commands = [
#       "/bin/bash",
#       "-c",
#       "pip install adlfs s3fs requests pandas aiohttp asyncio datetime numpy prefect_azure prefect pathlib ; prefect agent start -p default-agent-pool -q test"
#     ]
#   }

#   depends_on = [databricks_cluster.cluster_project]
# }

