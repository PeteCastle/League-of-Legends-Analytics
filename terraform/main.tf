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
  latest = true
  long_term_support = true
  ml = false
  gpu = false
  scala = "2.12"
  spark_version = "3.3.2"

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
  }
  custom_tags = {
    "ResourceClass" = "SingleNode"
  }
}