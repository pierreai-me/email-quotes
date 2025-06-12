#!/bin/bash

# Exit on error
set -e

# Usage: ./setup_azure.sh <resource-group-name> [location]
# Cleanup: az group delete --name <resource-group-name> --yes --no-wait

# Check if resource group name is provided
if [ $# -eq 0 ]; then
    echo "Usage: $0 <resource-group-name> [location]"
    echo "Example: $0 emailquotes001 eastus2"
    exit 1
fi

# Parameters
RESOURCE_GROUP=$1
LOCATION=${2:-eastus2}

# Validate resource group name
if [[ ! "$RESOURCE_GROUP" =~ ^[a-z0-9]+$ ]]; then
    echo "Error: Resource group name must contain only lowercase letters and numbers"
    echo "Example: emailquotes001"
    exit 1
fi

if [ ${#RESOURCE_GROUP} -gt 20 ]; then
    echo "Error: Resource group name must be 20 characters or less"
    echo "Current length: ${#RESOURCE_GROUP}"
    exit 1
fi

# Generate resource names based on resource group
EVENTHUB_NAMESPACE="${RESOURCE_GROUP}evhub"
SQL_SERVER_NAME="${RESOURCE_GROUP}sql"
COSMOS_ACCOUNT="${RESOURCE_GROUP}cosmos"
FUNCTION_APP_NAME="${RESOURCE_GROUP}api"
STORAGE_ACCOUNT="${RESOURCE_GROUP}stor"

# Generate secure random password
SQL_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
SQL_USERNAME="candidateuser"
SQL_DATABASE="interviewdb"
EVENTHUB_NAME="bond-quotes"
COSMOS_DATABASE="InterviewDB"
COSMOS_CONTAINER="BondQuotes"

with_retry() {
    local max_retries=3
    local retry_delay=10

    for i in $(seq 1 $max_retries); do
        if "$@"; then
            sleep 3  # Brief pause for propagation
            return 0
        else
            if [ $i -lt $max_retries ]; then
                sleep $retry_delay
            fi
        fi
    done
    return 1
}

# Enable command echoing
set -x

# Create Resource Group
with_retry az group create --name "$RESOURCE_GROUP" --location "$LOCATION"

# Create Event Hubs (Kafka)
with_retry az eventhubs namespace create \
  --name "$EVENTHUB_NAMESPACE" \
  --resource-group "$RESOURCE_GROUP" \
  --sku Basic \
  --location "$LOCATION"

with_retry az eventhubs eventhub create \
  --name "$EVENTHUB_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --partition-count 2 \
  --retention-time-in-hours 24 \
  --cleanup-policy Delete

EVENTHUB_CONNECTION_STRING=$(az eventhubs namespace authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$EVENTHUB_NAMESPACE" \
  --name RootManageSharedAccessKey \
  --query primaryConnectionString -o tsv)

# Create SQL Database and configure firewall rules
with_retry az sql server create \
  --name "$SQL_SERVER_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --admin-user "$SQL_USERNAME" \
  --admin-password "$SQL_PASSWORD"

with_retry az sql db create \
  --resource-group "$RESOURCE_GROUP" \
  --server "$SQL_SERVER_NAME" \
  --name "$SQL_DATABASE" \
  --service-objective Basic \
  --backup-storage-redundancy Local

with_retry az sql server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --server "$SQL_SERVER_NAME" \
  --name AllowAzureServices \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 0.0.0.0

with_retry az sql server firewall-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --server "$SQL_SERVER_NAME" \
  --name AllowAllInterviewIPs \
  --start-ip-address 0.0.0.0 \
  --end-ip-address 255.255.255.255

# Create NoSQL DB (Cosmos)
with_retry az cosmosdb create \
  --name "$COSMOS_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --locations regionName="$LOCATION" \
  --capabilities EnableServerless \
  --enable-free-tier false

with_retry az cosmosdb sql database create \
  --account-name "$COSMOS_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --name "$COSMOS_DATABASE"

with_retry az cosmosdb sql container create \
  --account-name "$COSMOS_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --database-name "$COSMOS_DATABASE" \
  --name "$COSMOS_CONTAINER" \
  --partition-key-path "/id"

COSMOS_ENDPOINT=$(az cosmosdb show \
  --name "$COSMOS_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query documentEndpoint -o tsv)

COSMOS_KEY=$(az cosmosdb keys list \
  --name "$COSMOS_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --query primaryMasterKey -o tsv)

# Create and configure Function App for REST API
# (i.e. if the candidate prefers to send GET requests instead of using Kafka client)
with_retry az storage account create \
  --name "$STORAGE_ACCOUNT" \
  --location "$LOCATION" \
  --resource-group "$RESOURCE_GROUP" \
  --sku Standard_LRS \
  --kind StorageV2

with_retry az functionapp create \
  --name "$FUNCTION_APP_NAME" \
  --storage-account "$STORAGE_ACCOUNT" \
  --resource-group "$RESOURCE_GROUP" \
  --consumption-plan-location "$LOCATION" \
  --runtime python \
  --runtime-version 3.9 \
  --functions-version 4 \
  --os-type Linux

# Configure Function App settings
with_retry az functionapp config appsettings set \
  --name "$FUNCTION_APP_NAME" \
  --resource-group "$RESOURCE_GROUP" \
  --settings \
    "EVENTHUB_CONNECTION_STRING=$EVENTHUB_CONNECTION_STRING" \
    "EVENTHUB_NAME=$EVENTHUB_NAME" \
    "COSMOS_ENDPOINT=$COSMOS_ENDPOINT" \
    "COSMOS_KEY=$COSMOS_KEY" \
    "COSMOS_DATABASE=$COSMOS_DATABASE" \
    "COSMOS_CONTAINER=$COSMOS_CONTAINER"

# Get Function App URL
FUNCTION_APP_URL="https://${FUNCTION_APP_NAME}.azurewebsites.net"

# Disable command echoing for file generation
set +x

# Generate environment file
ENV_FILE="${RESOURCE_GROUP}.env"
cat > "$ENV_FILE" << EOF
# =========================================
# Azure Interview Environment Credentials
# =========================================
# Generated: $(date)
# Resource Group: $RESOURCE_GROUP
# Location: $LOCATION
# =========================================

# Event Hubs (Kafka-compatible)
EVENTHUB_CONNECTION_STRING="${EVENTHUB_CONNECTION_STRING}"
EVENTHUB_NAME="${EVENTHUB_NAME}"
EVENTHUB_NAMESPACE="${EVENTHUB_NAMESPACE}"
KAFKA_BROKER="${EVENTHUB_NAMESPACE}.servicebus.windows.net:9093"

# SQL Database
SQL_CONNECTION_STRING="Server=tcp:${SQL_SERVER_NAME}.database.windows.net,1433;Initial Catalog=${SQL_DATABASE};Persist Security Info=False;User ID=${SQL_USERNAME};Password=${SQL_PASSWORD};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
SQL_SERVER="${SQL_SERVER_NAME}.database.windows.net"
SQL_DATABASE="${SQL_DATABASE}"
SQL_USERNAME="${SQL_USERNAME}"
SQL_PASSWORD="${SQL_PASSWORD}"

# Cosmos DB (NoSQL)
COSMOS_ENDPOINT="${COSMOS_ENDPOINT}"
COSMOS_KEY="${COSMOS_KEY}"
COSMOS_DATABASE="${COSMOS_DATABASE}"
COSMOS_CONTAINER="${COSMOS_CONTAINER}"
COSMOS_CONNECTION_STRING="AccountEndpoint=${COSMOS_ENDPOINT};AccountKey=${COSMOS_KEY};"

# REST API
API_ENDPOINT="${FUNCTION_APP_URL}/api/quote"
FUNCTION_APP_URL="${FUNCTION_APP_URL}"

# Azure Info
AZURE_REGION="${LOCATION}"
AZURE_RESOURCE_GROUP="${RESOURCE_GROUP}"
EOF
