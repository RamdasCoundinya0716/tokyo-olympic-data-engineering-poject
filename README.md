**Documentation for Tokyo Olympic Data Processing**

**Introduction:**
This PySpark script processes Tokyo Olympic data stored in Azure Data Lake Storage using Azure Databricks. It covers data loading, transformation, and storage.

**Azure Data Lake Mounting:**
- The script mounts the Azure Data Lake Storage using the provided configurations, allowing seamless access to the Olympic data.

```python
# Initialize Variables
container_name = "enter_your_container_name_here"
storage_account_name = "enter_your_storage_account_name_here"
tenant_id = "enter_your_tenant_id_here"
client_id = "enter_your_client_id_here"
client_secret = "enter_your_client_secret_here"

# Azure Data Lake Storage Mounting Configuration
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Mount Azure Data Lake Storage
dbutils.fs.mount(
    source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point="/mnt/tokyoolymic",
    extra_configs=configs
)
```

**Data Loading:**
- The script loads CSV data into Spark DataFrames for athletes, coaches, entries by gender, medals, and teams.

```python
# Data Loading
athletes = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolymic/raw-data/teams.csv")
```

**Data Exploration:**
- The script displays the schema and sample data for each DataFrame.

```python
# Data Exploration
athletes.show()
athletes.printSchema()

coaches.show()
coaches.printSchema()

entriesgender.show()
entriesgender.printSchema()

medals.show()
medals.printSchema()

teams.show()
teams.printSchema()
```

**Data Transformation:**
- The script transforms the 'entriesgender' DataFrame, converting columns to appropriate data types.

```python
# Data Transformation
entriesgender = entriesgender.withColumn("Female", col("Female").cast(IntegerType())) \
    .withColumn("Male", col("Male").cast(IntegerType())) \
    .withColumn("Total", col("Total").cast(IntegerType()))
entriesgender.printSchema()
```

**Data Analysis:**
- The script performs analysis tasks, such as finding top countries with the highest number of gold medals and calculating average entries by gender for each discipline.

```python
# Data Analysis
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("Team_Country", "Gold").show()

average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()
```

**Data Storage:**
- The script writes the transformed DataFrames to Azure Data Lake Storage.

```python
# Data Storage
athletes.repartition(1).write.mode("overwrite").option("header", 'true').csv("/mnt/tokyoolymic/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolymic/transformed-data/teams")
```

**Conclusion:**
This script provides a comprehensive overview of the data processing workflow for Tokyo Olympic data, from loading to transformation and storage, using PySpark on Azure Databricks.