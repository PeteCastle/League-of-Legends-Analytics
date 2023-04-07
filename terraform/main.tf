terraform {
  required_version = ">= 1.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.0"
    }
  }
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