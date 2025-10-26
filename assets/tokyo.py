# %%
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# %%
dbutils.fs.unmount("/mnt/tokyoolymic")


# %%
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "",
"fs.azure.account.oauth2.client.secret": '',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/tenant-id/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicdata16.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolympic", # choose name you want "/mnt/xyz"
extra_configs = configs)

# %%
%fs
ls "/mnt/tokyoolympic"

# %%
spark

# %%
athletes = spark.read.format("csv").load("/mnt/tokyoolympic/raw-data/athletes.csv")

# %%
athletes.show()

# %%
athletes = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympic/raw-data/teams.csv")

# %%
athletes.show()

# %%
teams.show()

# %%
athletes.printSchema()

# %%
coaches.show()

# %%
coaches.printSchema()

# %%
entriesgender.show()

# %%
entriesgender.printSchema()

# %%
entriesgender = entriesgender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))

# %%
entriesgender.printSchema()

# %%
medals.show()

# %%
medals.printSchema()

# %%
# Instead of manually writing new set of codes to tranform the data from from string to interger, use the below code at the point of reading the tables:

athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
entriesgender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/entriesgender.csv")
medals = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolympic/raw-data/teams.csv")

# %%
medals.printSchema()

# %%
teams.show()

# %%
teams.printSchema()

# %%
# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).show()

# %%
# Find the top countries with the highest number of gold medals
top_gold_medal_countries = medals.orderBy("Gold", ascending=False).select("TeamCountry", "Gold").show()

# %%
# Calculate the average number of entries by gender for each discipline
average_entries_by_gender = entriesgender.withColumn(
    'Avg_Female', entriesgender['Female'] / entriesgender['Total']
).withColumn(
    'Avg_Male', entriesgender['Male'] / entriesgender['Total']
)
average_entries_by_gender.show()

# %%
athletes.write.option("header", "true").csv("/mnt/tokyoolympic/transformed-data/athletes")

# %%
athletes.write.option("header", "true").csv("/mnt/tokyoolympic/transform-data/athletes")

# %%
athletes.write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/athletes")

# %%
athletes.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/athletes")

# %%
coaches.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/coaches")
entriesgender.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/entriesgender")
medals.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/medals")
teams.repartition(1).write.mode("overwrite").option("header", "true").csv("/mnt/tokyoolympic/transform-data/teams")


