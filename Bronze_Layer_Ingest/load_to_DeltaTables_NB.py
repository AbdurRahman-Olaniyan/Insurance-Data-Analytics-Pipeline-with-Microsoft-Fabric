#!/usr/bin/env python
# coding: utf-8

# ## load_to_DeltaTables_NB
# 
# New notebook

# In[5]:


from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, input_file_name

BASE = "Files/bronze/"

def load_csv_as_string(path: str, schema: StructType):
    return (spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            .load(path)
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_source_file", input_file_name())
            )


# In[6]:


incident_schema = StructType([
    StructField("IncidentID", StringType(), True),
    StructField("ClaimID", StringType(), True),
    StructField("IncidentDate", StringType(), True),
    StructField("IncidentType", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Description", StringType(), True),
])

renewal_schema = StructType([
    StructField("RenewalID", StringType(), True),
    StructField("PolicyNo", StringType(), True),
    StructField("RenewalDate", StringType(), True),
    StructField("Amount", StringType(), True),
    StructField("PaymentMethod", StringType(), True),
])

agent_schema = StructType([
    StructField("AgentID", StringType(), True),
    StructField("AgentName", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("HireDate", StringType(), True),
    StructField("PerformanceRating", StringType(), True),
])

customer_schema = StructType([
    StructField("CustomerID", StringType(), True),
    StructField("FullName", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("Phone", StringType(), True),
    StructField("JoinDate", StringType(), True),
])

date_schema = StructType([
    StructField("DateKey", StringType(), True),
    StructField("FullDate", StringType(), True),
    StructField("Year", StringType(), True),
    StructField("Quarter", StringType(), True),
    StructField("Month", StringType(), True),
    StructField("MonthName", StringType(), True),
    StructField("Day", StringType(), True),
    StructField("DayOfWeek", StringType(), True),
])

product_schema = StructType([
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("Premium", StringType(), True),
    StructField("CoverageType", StringType(), True),
    StructField("DurationMonths", StringType(), True),
])


# In[7]:


# incident
df = load_csv_as_string(BASE + "azureblob/incident.csv", incident_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_incident")


# In[8]:


# renewal
df = load_csv_as_string(BASE + "azureblob/renewal.csv", renewal_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_renewal")


# In[9]:


# agent
df = load_csv_as_string(BASE + "github/agent.csv", agent_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_agent")


# In[10]:


# customer
df =load_csv_as_string(BASE + "github/customer.csv", customer_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_customer")


# In[11]:


# date
df = load_csv_as_string(BASE + "github/date.csv", date_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_date")


# In[12]:


# product
df = load_csv_as_string(BASE + "github/product.csv", product_schema)
df.write.format("delta").mode("overwrite").saveAsTable("stg_product")


# In[16]:


tables = ["stg_incident", "stg_renewal", "stg_agent", "stg_customer", "stg_date", "stg_product"]

for t in tables:
    print("\n" + "="*40)
    print("TABLE:", t)
    print("ROWS :", spark.table(t).count())
    spark.table(t).printSchema()
    spark.table(t).show(5, truncate=False)

