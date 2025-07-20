# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option("inferSchema", "true").option("header", "true").option('multiline', 'False').load('/Volumes/workspace/study/study/drivers.json')

# COMMAND ----------

df_json.limit(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/study/study/')

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load('/Volumes/workspace/study/study/BigMart Sales.csv')

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
            Item_Identifier STRING,
            Item_Weight STRING,
            Item_Fat_Content STRING,
            Item_Visibility double,
            Item_Type STRING,
            Item_MRP double,
            Outlet_Identifier STRING,
            Outlet_Establishment_Year INTEGER,
            Outlet_Size STRING,
            Outlet_Location_Type STRING,
            Outlet_Type STRING,
            Item_Outlet_Sales double
'''

# COMMAND ----------

df = spark.read.format("csv")\
            .schema(my_ddl_schema)\
            .option("header", "true")\
            .load('/Volumes/workspace/study/study/BigMart Sales.csv')


# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

my_struct_schema = StructType([
    StructField('Item_Identifier', StringType(), True),
    StructField('Item_Weight', StringType(), True),
    StructField('Item_Fat_Content', StringType(), True),
    StructField('Item_Visibility', StringType(), True),
    StructField('Item_Type', StringType(), True),
    StructField('Item_MRP', StringType(), True),
    StructField('Outlet_Identifier', StringType(), True),
    StructField('Outlet_Establishment_Year', StringType(), True),
    StructField('Outlet_Size', StringType(), True),
    StructField('Outlet_Location_Type', StringType(), True),
    StructField('Outlet_Type', StringType(), True),
    StructField('Item_Outlet_Sales', StringType(), True),
])

# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_struct_schema)\
            .option("header", "true")\
            .load('/Volumes/workspace/study/study/BigMart Sales.csv')


# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT 

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel = df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1 

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 

# COMMAND ----------

df.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 3 

# COMMAND ----------

# MAGIC %md
# MAGIC - .isin() - Checks if a column's value is one of several specified values
# MAGIC - .isNull() - Checks if a column's value is null

# COMMAND ----------

df.filter(
    (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')) & 
    (col('Outlet_Size').isNull())
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_Wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### withColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### scenario 1 New col
# MAGIC - lit() - function creates a literal constant column

# COMMAND ----------

df = df.withColumn("flag", lit("new"))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('muliply', col('Item_Weight') * col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 modify col

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_content'),'Regular', 'Reg'))\
    .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_content'),'Low Fat', 'LF')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort/OrderBy

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1 

# COMMAND ----------

df.sort(col("Item_Weight").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 3 

# COMMAND ----------

df.sort(col('Item_Weight').desc(), col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

df.drop("Item_Fat_Content").display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 

# COMMAND ----------

df.drop('Item_Visibility','Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP_DUPLICATES

# COMMAND ----------

df.drop_duplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION and UNION BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prepering DataFrames

# COMMAND ----------

data1 = [
  (1, "kad"),
  (2, 'sid')
]
schema1 = 'ID STRING, NAME STRING'

df1 = spark.createDataFrame(data1, schema1)

data2 = [
  (3, 'rahul'),
  (4, 'jas')
]
schema2 = 'ID STRING, NAME STRING'

df2 = spark.createDataFrame(data2, schema2)



# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [
  ("kad", 1),
  ('sid', 2)
]
schema1 = 'NAME STRING, ID STRING'

df1 = spark.createDataFrame(data1, schema1)

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### STRING DUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC - INITCAP()
# MAGIC - LOWER()
# MAGIC - UPPER()

# COMMAND ----------

df.select(initcap(col('Item_Type')).alias('Item_Type')).display()

# COMMAND ----------

df.select(lower(col('Item_Type'))).display()

# COMMAND ----------

df.select(upper(col('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FUNCTION
# MAGIC - CURRENT_DATE()
# MAGIC - DATE_ADD()
# MAGIC - DATE_SUB()

# COMMAND ----------

df = df.withColumn("curr date", current_date())
df.display()

# COMMAND ----------

df = df.withColumn('week after', date_add('curr date', 7))
df.display()

# COMMAND ----------

df.withColumn('week before', date_sub('curr date', 7)).display()


# COMMAND ----------

df = df.withColumn('week before', date_add('curr date', -7))
df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### DATEDIFF

# COMMAND ----------

df = df.withColumn('datediff',date_diff('week after', 'curr date'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE_FORMAT

# COMMAND ----------

df = df.withColumn('week before', date_format('week before', "dd-MM-yyyy"))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### HANDLING NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1 - dropping nuls 

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 - Fill nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT and INDEXING

# COMMAND ----------

df.withColumn('Outlet_Type', split(col('Outlet_Type'), ' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type', split(col('Outlet_Type'), ' ')[1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE()

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type', split(col('Outlet_Type'), ' '))
df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS

# COMMAND ----------

df_exp.withColumn('Type1_flag', array_contains("Outlet_Type", 'Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP_BY

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2 

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(mean('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 3

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 4 

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size').agg(sum('Item_MRP').alias('Total_MRP'), avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size') \
    .agg(
        round(sum('Item_MRP'),2).alias('Total_MRP'),
        round(avg('Item_MRP'),2).alias('Avg_MRP')
    ) \
    .orderBy("Item_Type", "Outlet_Size", ascending=[True, False]) \
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### COLLECT_LIST

# COMMAND ----------

data = [('user1', 'book1'),
        ('user1', 'book2'),
        ('user2', 'book2'),
        ('user2', 'book4'),
        ('user3', 'book1')]
schema = 'user string, book string'
df_book = spark.createDataFrame(data, schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(round(avg('Item_MRP'), 2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN - OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 1 

# COMMAND ----------

df = df.withColumn('veg_flag', when(col('Item_Type') == 'Meat', 'Non_Veg').otherwise('Veg'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario 2

# COMMAND ----------

df.withColumn('veg_exp_flag', when(((col('veg_flag') == 'Veg') & (col('Item_MRP') < 100 )), 'Veg_Inexpensive')\
                             .when((col('veg_flag') == 'Veg') & (col('Item_MRP') > 100 ), 'Veg_Expensive')\
                             .otherwise('Non_Veg'))\
                             .display()      

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------


dataj1 = [
    ('1', 'gaur', 'd01'),
    ('2', 'kit', 'd02'),
    ('3', 'sam', 'd03'),
    ('4', 'tim', 'd03'),
    ('5', 'aman', 'd05'),
    ('6', 'nad', 'd06')
]

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'

df1 = spark.createDataFrame(dataj1, schemaj1)

dataj2 = [
    ('d01', 'HR'),
    ('d02', 'Marketing'),
    ('d03', 'Accounts'),
    ('d04', 'IT'),
    ('d05', 'Finance')
]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2, schemaj2)

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER JOIN

# COMMAND ----------

df1.join(df2, on='dept_id', how='inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### LEFT JOIN

# COMMAND ----------

df1.join(df2, on='dept_id', how='left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RIGHT JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], how='right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI JOIN
# MAGIC

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], how='anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTION

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW NUMBER

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import Window

# COMMAND ----------

df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### RANK

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DENSE RANK

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
  .withColumn('dense_rank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()  

# COMMAND ----------

# MAGIC %md
# MAGIC #### CUMULATIVE SUM

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTION - UDF 

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 1
# MAGIC

# COMMAND ----------

def my_func(x):
  return x * x 

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 2

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

# MAGIC %md
# MAGIC #### STEP 3

# COMMAND ----------

df.withColumn('my_new_column', my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV

# COMMAND ----------

df.write.format('csv')\
    .save('/Volumes/workspace/study/study/data.csv')


# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING MODES

# COMMAND ----------

# MAGIC %md
# MAGIC #### APPEND

# COMMAND ----------

df.write.format('csv')\
  .mode('append')\
  .save('/Volumes/workspace/study/study/data.csv')

# COMMAND ----------

df.write.format('csv') \
    .mode('append') \
    .option('path', '/Volumes/workspace/study/study/data.csv') \
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OVERWRITE

# COMMAND ----------

df.write.format('csv')\
  .mode('overwrite') \
  .option('path', '/Volumes/workspace/study/study/data.csv') \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ERROR

# COMMAND ----------

df.write.format('csv')\
  .mode('error') \
  .option('path', '/Volumes/workspace/study/study/data.csv') \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### IGNORE

# COMMAND ----------

df.write.format('csv')\
  .mode('ignore') \
  .option('path', '/Volumes/workspace/study/study/data.csv') \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET

# COMMAND ----------

df.write.format('parquet')\
  .mode('overwrite') \
  .option('path', '/Volumes/workspace/study/study/data.csv') \
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ####TABLE

# COMMAND ----------

df.write.format('delta') \
  .mode('overwrite') \
  .saveAsTable('my_table')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * 
# MAGIC FROM my_view
# MAGIC WHERE Item_Fat_Content = 'Low Fat'

# COMMAND ----------

df_sql = spark.sql("SELECT * FROM my_view WHERE Item_Fat_Content = 'Low Fat' ")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

