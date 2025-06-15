# Azure Infrastructure Makefile
# Usage:
#   make azure RESOURCE_GROUP=emailquotes002 [LOCATION=eastus2]
#   make delete-azure RESOURCE_GROUP=mygroup001

# Default values
LOCATION ?= eastus2
CLOUD_DIR := ./cloud
TOUCH_DIR := $(CLOUD_DIR)/$(RESOURCE_GROUP)

# Validate RESOURCE_GROUP
ifndef RESOURCE_GROUP
$(error RESOURCE_GROUP is required. Usage: make azure RESOURCE_GROUP=emailquotes002)
endif

# Validate resource group name
ifneq ($(shell echo "$(RESOURCE_GROUP)" | grep -E '^[a-z0-9]+$$'),$(RESOURCE_GROUP))
$(error Resource group name must contain only lowercase letters and numbers)
endif

ifeq ($(shell test $(shell echo -n "$(RESOURCE_GROUP)" | wc -c) -gt 20 && echo "too_long"),too_long)
$(error Resource group name must be 20 characters or less)
endif

# Generate resource names
EVENTHUB_NAMESPACE := $(RESOURCE_GROUP)evhub
POSTGRES_SERVER_NAME := $(RESOURCE_GROUP)pg
COSMOS_ACCOUNT := $(RESOURCE_GROUP)cosmos
FUNCTION_APP_NAME := $(RESOURCE_GROUP)api
STORAGE_ACCOUNT := $(RESOURCE_GROUP)stor

# Fixed names
POSTGRES_USERNAME := candidateuser
POSTGRES_DATABASE := interviewdb
EVENTHUB_NAME := bond-quotes
COSMOS_DATABASE := InterviewDB
COSMOS_CONTAINER := BondQuotes

# Generate reproducible password using resource group as seed
POSTGRES_PASSWORD := $(shell echo "$(RESOURCE_GROUP)" | openssl dgst -sha256 -binary | openssl base64 | tr -d "=+/" | cut -c1-25)

# Environment file in the cloud directory
ENV_FILE := $(TOUCH_DIR)/$(RESOURCE_GROUP).env

# No retry logic - just fail fast and let user rerun if needed

# Create cloud directory structure
$(CLOUD_DIR):
	@mkdir -p $(CLOUD_DIR)

$(TOUCH_DIR): | $(CLOUD_DIR)
	@mkdir -p $(TOUCH_DIR)

# Top-level targets
.PHONY: azure delete-azure clean clean-all list-deployments

azure: $(ENV_FILE)

delete-azure:
	@echo "Deleting resource group $(RESOURCE_GROUP)..."
	az group delete --name "$(RESOURCE_GROUP)" --yes --no-wait
	rm -rf $(TOUCH_DIR)

clean:
	rm -rf $(TOUCH_DIR)

clean-all:
	rm -rf $(CLOUD_DIR)

list-deployments:
	@echo "Available deployments:"
	@if [ -d $(CLOUD_DIR) ]; then \
		find $(CLOUD_DIR) -maxdepth 1 -type d -not -path $(CLOUD_DIR) -exec basename {} \; | sort; \
	else \
		echo "No deployments found."; \
	fi

# Resource Group
$(TOUCH_DIR)/resource-group: | $(TOUCH_DIR)
	@echo "Creating resource group $(RESOURCE_GROUP)..."
	az group create --name "$(RESOURCE_GROUP)" --location "$(LOCATION)"
	@touch $@

# Event Hubs Namespace (depends on resource group)
$(TOUCH_DIR)/eventhub-namespace: $(TOUCH_DIR)/resource-group
	@echo "Creating Event Hubs namespace $(EVENTHUB_NAMESPACE)..."
	az eventhubs namespace create \
		--name "$(EVENTHUB_NAMESPACE)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--sku Standard \
		--location "$(LOCATION)"
	@touch $@

# Event Hub (depends on namespace)
$(TOUCH_DIR)/eventhub: $(TOUCH_DIR)/eventhub-namespace
	@echo "Creating Event Hub $(EVENTHUB_NAME)..."
	az eventhubs eventhub create \
		--name "$(EVENTHUB_NAME)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--namespace-name "$(EVENTHUB_NAMESPACE)" \
		--partition-count 2 \
		--retention-time-in-hours 24 \
		--cleanup-policy Delete
	@touch $@

# PostgreSQL Server (depends on resource group)
$(TOUCH_DIR)/postgres-server: $(TOUCH_DIR)/resource-group
	@echo "Creating PostgreSQL Server $(POSTGRES_SERVER_NAME)..."
	az postgres flexible-server create \
		--name "$(POSTGRES_SERVER_NAME)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--location "$(LOCATION)" \
		--admin-user "$(POSTGRES_USERNAME)" \
		--admin-password "$(POSTGRES_PASSWORD)" \
		--sku-name Standard_B1ms \
		--tier Burstable \
		--storage-size 32 \
		--version 17 \
		--public-access All
	@touch $@

# PostgreSQL Database (depends on PostgreSQL server)
$(TOUCH_DIR)/postgres-database: $(TOUCH_DIR)/postgres-server
	@echo "Creating PostgreSQL Database $(POSTGRES_DATABASE)..."
	az postgres flexible-server db create \
		--resource-group "$(RESOURCE_GROUP)" \
		--server-name "$(POSTGRES_SERVER_NAME)" \
		--database-name "$(POSTGRES_DATABASE)"
	@touch $@

# PostgreSQL Firewall Rules (depends on PostgreSQL server)
$(TOUCH_DIR)/postgres-firewall: $(TOUCH_DIR)/postgres-server
	@echo "Configuring PostgreSQL Server firewall rules..."
	az postgres flexible-server firewall-rule create \
		--resource-group "$(RESOURCE_GROUP)" \
		--name "$(POSTGRES_SERVER_NAME)" \
		--rule-name AllowAllInterviewIPs \
		--start-ip-address 0.0.0.0 \
		--end-ip-address 255.255.255.255
	@touch $@

# Cosmos DB Account (depends on resource group)
$(TOUCH_DIR)/cosmos-account: $(TOUCH_DIR)/resource-group
	@echo "Creating Cosmos DB account $(COSMOS_ACCOUNT)..."
	az cosmosdb create \
		--name "$(COSMOS_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--locations regionName="$(LOCATION)" \
		--capabilities EnableServerless \
		--enable-free-tier false
	@touch $@

# Cosmos DB Database (depends on account)
$(TOUCH_DIR)/cosmos-database: $(TOUCH_DIR)/cosmos-account
	@echo "Creating Cosmos DB database $(COSMOS_DATABASE)..."
	az cosmosdb sql database create \
		--account-name "$(COSMOS_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--name "$(COSMOS_DATABASE)"
	@touch $@

# Cosmos DB Container (depends on database)
$(TOUCH_DIR)/cosmos-container: $(TOUCH_DIR)/cosmos-database
	@echo "Creating Cosmos DB container $(COSMOS_CONTAINER)..."
	az cosmosdb sql container create \
		--account-name "$(COSMOS_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--database-name "$(COSMOS_DATABASE)" \
		--name "$(COSMOS_CONTAINER)" \
		--partition-key-path "/id"
	@touch $@

# Storage Account (depends on resource group)
$(TOUCH_DIR)/storage-account: $(TOUCH_DIR)/resource-group
	@echo "Creating storage account $(STORAGE_ACCOUNT)..."
	az storage account create \
		--name "$(STORAGE_ACCOUNT)" \
		--location "$(LOCATION)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--sku Standard_LRS \
		--kind StorageV2
	@touch $@

# Function App (depends on storage account)
$(TOUCH_DIR)/function-app: $(TOUCH_DIR)/storage-account
	@echo "Creating Function App $(FUNCTION_APP_NAME)..."
	az functionapp create \
		--name "$(FUNCTION_APP_NAME)" \
		--storage-account "$(STORAGE_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--consumption-plan-location "$(LOCATION)" \
		--runtime python \
		--runtime-version 3.12 \
		--functions-version 4 \
		--os-type Linux
	@touch $@

# Get Event Hub Connection String (depends on Event Hub)
$(TOUCH_DIR)/eventhub-connection: $(TOUCH_DIR)/eventhub
	@echo "Getting Event Hub connection string..."
	@az eventhubs namespace authorization-rule keys list \
		--resource-group "$(RESOURCE_GROUP)" \
		--namespace-name "$(EVENTHUB_NAMESPACE)" \
		--name RootManageSharedAccessKey \
		--query primaryConnectionString -o tsv > $(TOUCH_DIR)/eventhub-connection-string
	@touch $@

# Get Cosmos DB Credentials (depends on Cosmos container)
$(TOUCH_DIR)/cosmos-credentials: $(TOUCH_DIR)/cosmos-container
	@echo "Getting Cosmos DB credentials..."
	@az cosmosdb show \
		--name "$(COSMOS_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--query documentEndpoint -o tsv > $(TOUCH_DIR)/cosmos-endpoint
	@az cosmosdb keys list \
		--name "$(COSMOS_ACCOUNT)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--query primaryMasterKey -o tsv > $(TOUCH_DIR)/cosmos-key
	@touch $@

# Configure Function App (depends on Function App and credentials)
$(TOUCH_DIR)/function-app-config: $(TOUCH_DIR)/function-app $(TOUCH_DIR)/eventhub-connection $(TOUCH_DIR)/cosmos-credentials
	@echo "Configuring Function App settings..."
	$(eval EVENTHUB_CONNECTION_STRING := $(shell cat $(TOUCH_DIR)/eventhub-connection-string))
	$(eval COSMOS_ENDPOINT := $(shell cat $(TOUCH_DIR)/cosmos-endpoint))
	$(eval COSMOS_KEY := $(shell cat $(TOUCH_DIR)/cosmos-key))
	az functionapp config appsettings set \
		--name "$(FUNCTION_APP_NAME)" \
		--resource-group "$(RESOURCE_GROUP)" \
		--settings \
			"EVENTHUB_CONNECTION_STRING=$(EVENTHUB_CONNECTION_STRING)" \
			"EVENTHUB_NAME=$(EVENTHUB_NAME)" \
			"KAFKA_BROKER=$(EVENTHUB_NAMESPACE).servicebus.windows.net:9093" \
			"COSMOS_ENDPOINT=$(COSMOS_ENDPOINT)" \
			"COSMOS_KEY=$(COSMOS_KEY)" \
			"COSMOS_DATABASE=$(COSMOS_DATABASE)" \
			"COSMOS_CONTAINER=$(COSMOS_CONTAINER)"
	@touch $@

# Deploy Function App Code (depends on function app config)
$(TOUCH_DIR)/function-app-deploy: $(TOUCH_DIR)/function-app-config
	@echo "Deploying Function App code..."
	# Create deployment package
	@echo "Creating deployment package..."
	@mkdir -p $(TOUCH_DIR)/deployment
	@cp -r solution/ $(TOUCH_DIR)/deployment/

	# Create host.json with routing
	@echo '{"version": "2.0", "logging": {"applicationInsights": {"samplingSettings": {"isEnabled": true}}}, "extensionBundle": {"id": "Microsoft.Azure.Functions.ExtensionBundle", "version": "[4.*, 5.0.0)"}, "extensions": {"http": {"routePrefix": ""}}}' > $(TOUCH_DIR)/deployment/host.json

	# Create function_app.py with Azure Functions v2 syntax
	@echo 'import azure.functions as func' > $(TOUCH_DIR)/deployment/function_app.py
	@echo 'from solution.azure.quote_server import app as fastapi_app' >> $(TOUCH_DIR)/deployment/function_app.py
	@echo '' >> $(TOUCH_DIR)/deployment/function_app.py
	@echo 'app = func.AsgiFunctionApp(' >> $(TOUCH_DIR)/deployment/function_app.py
	@echo '    app=fastapi_app,' >> $(TOUCH_DIR)/deployment/function_app.py
	@echo '    http_auth_level=func.AuthLevel.ANONYMOUS' >> $(TOUCH_DIR)/deployment/function_app.py
	@echo ')' >> $(TOUCH_DIR)/deployment/function_app.py

	# Create complete requirements.txt
	@echo 'azure-functions' > $(TOUCH_DIR)/deployment/requirements.txt
	@cat requirements.txt >> $(TOUCH_DIR)/deployment/requirements.txt

	# Try direct deployment using Azure Functions Core Tools instead of zip
	@echo "Installing Azure Functions Core Tools if not present..."
	@which func || npm install -g azure-functions-core-tools@4 --unsafe-perm true

	@echo "Deploying using Azure Functions Core Tools..."
	cd $(TOUCH_DIR)/deployment && func azure functionapp publish $(FUNCTION_APP_NAME) --python

	@touch $@

# Generate Environment File (depends on all infrastructure)
$(ENV_FILE): $(TOUCH_DIR)/postgres-database $(TOUCH_DIR)/postgres-firewall $(TOUCH_DIR)/function-app-deploy
	@echo "Generating environment file $(ENV_FILE)..."
	$(eval EVENTHUB_CONNECTION_STRING := $(shell cat $(TOUCH_DIR)/eventhub-connection-string))
	$(eval COSMOS_ENDPOINT := $(shell cat $(TOUCH_DIR)/cosmos-endpoint))
	$(eval COSMOS_KEY := $(shell cat $(TOUCH_DIR)/cosmos-key))
	@echo "# =========================================" > $(ENV_FILE)
	@echo "# Azure Interview Environment Credentials" >> $(ENV_FILE)
	@echo "# =========================================" >> $(ENV_FILE)
	@echo "# Generated: $$(date)" >> $(ENV_FILE)
	@echo "# Resource Group: $(RESOURCE_GROUP)" >> $(ENV_FILE)
	@echo "# Location: $(LOCATION)" >> $(ENV_FILE)
	@echo "# =========================================" >> $(ENV_FILE)
	@echo "" >> $(ENV_FILE)
	@echo "# Event Hubs (Kafka-compatible)" >> $(ENV_FILE)
	@echo "EVENTHUB_CONNECTION_STRING=\"$(EVENTHUB_CONNECTION_STRING)\"" >> $(ENV_FILE)
	@echo "EVENTHUB_NAME=\"$(EVENTHUB_NAME)\"" >> $(ENV_FILE)
	@echo "EVENTHUB_NAMESPACE=\"$(EVENTHUB_NAMESPACE)\"" >> $(ENV_FILE)
	@echo "KAFKA_BROKER=\"$(EVENTHUB_NAMESPACE).servicebus.windows.net:9093\"" >> $(ENV_FILE)
	@echo "" >> $(ENV_FILE)
	@echo "# PostgreSQL Database" >> $(ENV_FILE)
	@echo "POSTGRES_HOST=\"$(POSTGRES_SERVER_NAME).postgres.database.azure.com\"" >> $(ENV_FILE)
	@echo "POSTGRES_DATABASE=\"$(POSTGRES_DATABASE)\"" >> $(ENV_FILE)
	@echo "POSTGRES_USERNAME=\"$(POSTGRES_USERNAME)\"" >> $(ENV_FILE)
	@echo "POSTGRES_PASSWORD=\"$(POSTGRES_PASSWORD)\"" >> $(ENV_FILE)
	@echo "POSTGRES_PORT=\"5432\"" >> $(ENV_FILE)
	@echo "POSTGRES_SSLMODE=\"require\"" >> $(ENV_FILE)
	@echo "" >> $(ENV_FILE)
	@echo "# Cosmos DB (NoSQL)" >> $(ENV_FILE)
	@echo "COSMOS_ENDPOINT=\"$(COSMOS_ENDPOINT)\"" >> $(ENV_FILE)
	@echo "COSMOS_KEY=\"$(COSMOS_KEY)\"" >> $(ENV_FILE)
	@echo "COSMOS_DATABASE=\"$(COSMOS_DATABASE)\"" >> $(ENV_FILE)
	@echo "COSMOS_CONTAINER=\"$(COSMOS_CONTAINER)\"" >> $(ENV_FILE)
	@echo "COSMOS_CONNECTION_STRING=\"AccountEndpoint=$(COSMOS_ENDPOINT);AccountKey=$(COSMOS_KEY);\"" >> $(ENV_FILE)
	@echo "" >> $(ENV_FILE)
	@echo "# REST API" >> $(ENV_FILE)
	@echo "API_ENDPOINT=\"https://$(FUNCTION_APP_NAME).azurewebsites.net/api/quote\"" >> $(ENV_FILE)
	@echo "FUNCTION_APP_URL=\"https://$(FUNCTION_APP_NAME).azurewebsites.net\"" >> $(ENV_FILE)
	@echo "" >> $(ENV_FILE)
	@echo "# Azure Info" >> $(ENV_FILE)
	@echo "AZURE_REGION=\"$(LOCATION)\"" >> $(ENV_FILE)
	@echo "AZURE_RESOURCE_GROUP=\"$(RESOURCE_GROUP)\"" >> $(ENV_FILE)
	@echo "Environment file created at: $(ENV_FILE)"