import os

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType

os.environ['PYSPARK_PYTHON'] = 'python'
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [1,2,3,4,5,6,7,8,9,10,11,12]
# rdd=spark.sparkContext.parallelize(data)
# print(spark.sparkContext)
# ---------------------------------------------------------------------------------------------------------------------

# show():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# columns = ["seqno", "Quote"]
# data = [("1", "Be the change that you wish to see in the world"),
#     ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
#     ("3", "The purpose of our lives is to be happy."),
#     ("4", "Be cool.")]
#
# df = spark.createDataFrame(data=data, schema=columns)
# df.show()   [displays only few content with all columns]
# df.show(truncate=False)      [displays total content with all columns]
# df.show(2, truncate=False)     [displays total content with top 2 columns]
# df.show(2, truncate=25)          [displays only 25 characters and top 2 columns]
# df.show(3, truncate=False, vertical=True) [displays the content vertically of top 3 columns]
# ----------------------------------------------------------------------------------------------------------------------
# select()
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("sparkByExample.com").getOrCreate()
# data = [("James","Smith","USA","CA"),
#     ("Michael","Rose","USA","NY"),
#     ("Robert","Williams","USA","CA"),
#     ("Maria","Jones","USA","FL")]
#
# columns = ["firstname", "lastname", "country", "state"]
# df = spark.createDataFrame(data, columns)
# df.select(df.firstname, df.lastname).show()
# df.select("firstname", "lastname")
# TO GET ALL COLUMNS:
# df.select(*columns).show()
# ---------------------------------------------------------------------------------------------------------------------

# collect()
# Note that collect() is an action hence it does not return a DataFrame instead,
# it returns data in an Array to the driver. Once the data is in an array,
# you can use python for loop to process it further.

import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
#
# dept = [("Finance", 10),
#         ("Marketing", 20),
#         ("Sales", 30),
#         ("IT", 40)]
# deptColumns = ["dept_name", "dept_id"]
# deptDF = spark.createDataFrame(dept, deptColumns)
# data_collect = deptDF.collect()
# for each in data_collect:
#     print(f"{each['dept_name'], str(each['dept_id'])}")

# ---------------------------------------------------------------------------------------------------------------------

# withcolumn()

# pySpark withColumn() is a transformation function of DataFrame which is used to change the value,
# convert the datatype of an existing column, create a new column, and many more. In this post,
# It will walk you through commonly used PySpark DataFrame column operations using withColumn() examples.

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [('James', 'Smith', '1991-04-01', 'M', 3000),
#   ('Michael', 'Rose', '2000-05-19', 'M', 4000),
#   ('Robert', 'Williams', '1978-09-05', 'M', 4000),
#   ('Maria', 'Anne', '1967-12-01', 'F', 4000),
#   ('Jen', 'Mary', '1980-02-17', 'F', -1)
# ]
#
#
# columns = ["firstname", "lastname", "dob", "gender", "salary"]
# df = spark.createDataFrame(data, columns)
# print(df.printSchema())
# df1 = df.withColumn("salary", col("salary").cast("Integer")) [changing data type of salary from long to integer]
# print(df1.printSchema())
# ---------------------------------------------------------------------------------------------------------------------

# filter()

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField
# from pyspark.sql.types import StringType, ArrayType
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("sparkByExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('fullname', StructType([
#         StructField('firstname', StringType(), True),
#         StructField('middlename', StringType(), True),
#         StructField('lastname', StringType(), True)
#     ])),
#     StructField('languages', ArrayType((StringType())), True),
#     StructField('state', StringType(), True),
#     StructField('gender', StringType(), True)
# ])
#
# df = spark.createDataFrame(data, schema)
#
# df.filter(df.state == "OH").show(truncate=False)
# df.filter(col("state") == "OH").show(truncate=False)
# df.filter("state == 'OH'").show(truncate=False)
# df.filter((df.state == 'OH') & (df.gender == 'M')).show(truncate=False)
# li = ["OH", "CA", "DE"]
# df.filter(df.state.isin(li)).show()

#  ---------------------------------------------------------------------------------------------------------------------

# distinct() and drop duplicates():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = [("James", "Sales", 3000),
#     ("Michael", "Sales", 4600),
#     ("Robert", "Sales", 4100),
#     ("Maria", "Finance", 3000),
#     ("James", "Sales", 3000),
#     ("Scott", "Finance", 3300),
#     ("Jen", "Finance", 3900),
#     ("Jeff", "Marketing", 3000),
#     ("Kumar", "Marketing", 2000),
#     ("Saif", "Sales", 4100)]
# columns = ["employee_name", "department", "salary"]
# df = spark.createDataFrame(data, columns)
# distinct = df.distinct()
# print("Distinct Count:", distinct.count())
# distinct.show(truncate=False)
#
# d1 = df.dropDuplicates()
# print("Distinct count:", d1.count())
# d1.show()

# ---------------------------------------------------------------------------------------------------------------------

# sort() and order by():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = simpleData = [("James", "Sales", "NY", 90000, 34, 10000), \
#     ("Michael", "Sales", "NY", 86000, 56, 20000), \
#     ("Robert", "Sales", "CA", 81000, 30, 23000), \
#     ("Maria", "Finance", "CA", 90000, 24, 23000), \
#     ("Raman", "Finance", "CA", 99000, 40, 24000), \
#     ("Scott", "Finance", "NY", 83000, 36, 19000), \
#     ("Jen", "Finance", "NY", 79000, 53, 15000), \
#     ("Jeff", "Marketing", "CA", 80000, 25, 18000), \
#     ("Kumar", "Marketing", "NY", 91000, 50, 21000) \
#   ]
# columns = ["employee_name", "department", "state", "salary", "age", "bonus"]
# df = spark.createDataFrame(data, columns)
# df.sort("department").show()
# df.sort(col("department")).show()
# df.orderBy("department").show()
# df.orderBy(col("department")).show()
# df.sort(df.department.asc()).show()
# df.orderBy(df.department.asc()).show()
# df.sort(df.department.desc()).show()
# df.orderBy(df.department.desc()).show()
# ----------------------------------------------------------------------------------------------------------------------
# Groupby():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum, avg, max
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# simple_data = [("James", "Sales", "NY", 90000, 34, 10000),
#     ("Michael", "Sales", "NY", 86000, 56, 20000),
#     ("Robert", "Sales", "CA", 81000, 30, 23000),
#     ("Maria", "Finance", "CA", 90000, 24, 23000),
#     ("Raman", "Finance", "CA", 99000, 40, 24000),
#     ("Scott", "Finance", "NY", 83000, 36, 19000),
#     ("Jen", "Finance", "NY", 79000, 53, 15000),
#     ("Jeff", "Marketing", "CA", 80000, 25, 18000),
#     ("Kumar", "Marketing", "NY", 91000, 50, 21000)
#   ]
# schema = ["employee_name", "department", "state", "salary", "age", "bonus"]
# df = spark.createDataFrame(simple_data, schema)
# df.show()
# df.groupBy("department").sum("bonus").show(truncate=False)

# df2 = df.groupby("department").count()
# df2.show()

# df3 = df.groupby("department").min("salary")
# df3.show()

# df4 = df.groupby("department").max("salary")
# df4.show()

# df5 = df.groupby("department").avg("salary")
# df5.show()

# df6 = df.groupby("department", "state").sum("salary", "bonus")
# df6.show()


# df7 = df.groupBy("department") \
#     .agg(sum("salary").alias("sum_salary"),
#          avg("salary").alias("avg_salary"),
#          sum("bonus").alias("sum_bonus"),
#          max("bonus").alias("max_bonus")
#      )\
#     .show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# Inner join()
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "jaswi", "company 1"],
#         ["3", "rohit", "company 2"],
#         ["4", "sri devi", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
# # df.show()
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
# # df1.show()
#
#
# df.join(df1, df.ID == df1.ID, "inner").show()

# ---------------------------------------------------------------------------------------------------------------------
# Left Join()

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "ojaswi", "company 1"],
#         ["3", "rohith", "company 2"],
#         ["4", "sridevi", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
#
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
#
# df.join(df1, df.ID == df1.ID,"left").show()

# ---------------------------------------------------------------------------------------------------------------------
# Right Join()

# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample.com").getOrCreate()
# data = [["1", "sravan", "company 1"],
#         ["2", "bob", "company 1"],
#         ["3", "rohit", "company 2"],
#         ["4", "sri", "company 1"],
#         ["5", "bobby", "company 1"]]
#
# columns = ["ID", "Name", "Company"]
# df = spark.createDataFrame(data, columns)
#
# data1 = [["1", "45000", "IT"],
#         ["2", "145000", "Manager"],
#         ["6", "45000", "HR"],
#         ["5", "34000", "Sales"]]
# columns1 = ["ID", "SALARY", "DEPARTMENT"]
#
# df1 = spark.createDataFrame(data1, columns1)
#
# final_df = df1.join(df, df1.ID == df.ID,"right")
# final_df.show()
# df2 = final_df.select(final_df.ID, final_df.Name, final_df.SALARY)
# df2.show()

# ---------------------------------------------------------------------------------------------------------------------
# withcolumn()
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "gangadhar", 25000), (2, "shyam", 52000)]
# columns = ("id", "name", "salary")
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("salary", col("salary").cast('Integer'))
# df2 = df.withColumn("salary", col('salary')*2)
# df3 = df2.withColumn("country", lit("India"))
# df4 = df3.withColumn("copiedsalarycolumn", col("salary"))
# df4.show()

# ---------------------------------------------------------------------------------------------------------------------

# withcolumnranamed()
# Data frames are immutable.This method will not change the original dataframe
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col,lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "gangadhar", 25000), (2, "shyam", 52000)]
# columns = ("id", "name", "salary")
# df = spark.createDataFrame(data, columns)
# df.withColumnRenamed("salary", "salaries").show()
# ---------------------------------------------------------------------------------------------------------------------
# struct type and struct field:

# structure type is a collection of  structure field ie list of structure fields
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# from pyspark.sql.functions import col
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", 3000), (2, "Mahesh", 4000)]
# schema = StructType([
#                      StructField(name="id", dataType=IntegerType()),
#                      StructField(name="name", dataType=StringType()),
#                      StructField(name="salary", dataType=StringType())
#                    ])
# df = spark.createDataFrame(data, schema=schema)
# df.show()
# df.printSchema()
# ---------------------------------------------------------------------------------------------------------------------

# nested columns:

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# data = [(1, ("Gangadhar", "sharma"), 5000), (2, ("Mahesh", "pilli"), 5000)]
# Structname = StructType([
#                          StructField(name="firstname", dataType=StringType()),
#                          StructField(name="lastname", dataType=StringType())
#              ])
# schema = StructType([
#                     StructField(name="id", dataType=IntegerType()),
#                     StructField(name="name", dataType=Structname),
#                     StructField(name="salary", dataType=IntegerType())
#                   ])
#
# df = spark.createDataFrame(data, schema)
# df.show()
# ----------------------------------------------------------------------------------------------------------------------

# Array Type Columns:


# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('name', StructType([
#         StructField('firstname', dataType=StringType()),
#         StructField('middlename', dataType=StringType()),
#         StructField('lastname', dataType=StringType())
#     ])),
#     StructField("languages", dataType=ArrayType(StringType())),
#     StructField("state", dataType=StringType()),
#     StructField("gender", dataType=StringType())
#     ])
#
# df = spark.createDataFrame(data, schema)
#
# df.printSchema()
# df.show(truncate=False)
# ---------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ("Java", "Scala", 123), "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB", "DBMS"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB", "JAVA"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB", "CSHARP"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB", 123], "OH", "M")
# ]
#
# schema = StructType([
#     StructField('name', StructType([
#         StructField('firstname', dataType=StringType()),
#         StructField('middlename', dataType=StringType()),
#         StructField('lastname', dataType=StringType())
#     ])),
#
#     StructField("languages", StructType([
#         StructField("primary", dataType=StringType()),
#         StructField("secondary", dataType=StringType()),
#         StructField("extra", dataType=StringType())
#     ])),
#     StructField("state", dataType=StringType()),
#     StructField("gender", dataType=StringType())
#     ])
#
# df = spark.createDataFrame(data, schema)
#
# df.printSchema()
# df.show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
# FEW METHODS IN ARRAY:
# 1. Explode(): it will give every element of array in a separate row
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", ["python", "sql", "pyspark"]), (2, "Mahesh", ["java", "javascript", "test cases"])]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skill", explode(df.skills))
# df1.show()

# ---+---------+--------------------+----------+
# | ID|     Name|              skills|     skill|
# +---+---------+--------------------+----------+
# |  1|Gangadhar|[python, sql, pys...|    python|
# |  1|Gangadhar|[python, sql, pys...|       sql|
# |  1|Gangadhar|[python, sql, pys...|   pyspark|
# |  2|   Mahesh|[java, javascript,...|      java|
# |  2|   Mahesh|[java, javascript,...| javascript|
# |  2|   Mahesh|[java, javascript,...|test cases|
# +---+---------+--------------------+----------+

# 2. Split():

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import split
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", "python, sql, pyspark"), (2, "Mahesh", "java, javascript, test cases")]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skill", split(df.skills, ","))
# df1.show(truncate=False)

# ---+---------+----------------------------+--------------------------------+
# |ID |Name     |skills                      |skill                           |
# +---+---------+----------------------------+--------------------------------+
# |1  |Gangadhar|python, sql, pyspark        |[python,  sql,  pyspark]        |
# |2  |Mahesh   |java, javascript, test cases|[java,  javascript,  test cases]|
# +---+---------+----------------------------+--------------------------------+


# 3. Array

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import array
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", "python", "sql", "pyspark"), (2, "Mahesh", "java", "javascript", "test cases")]
# columns = ["ID", "Name", "primary", "secondary", "extra"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("skills", array(df.primary, df.secondary, df.extra))
# df1.show(truncate=False)

# ---+---------+-------+---------+----------+--------------------+
# | ID|     Name|primary|secondary|     extra|              skills|
# +---+---------+-------+---------+----------+--------------------+
# |  1|Gangadhar| python|      sql|   pyspark|[python, sql, pys...|
# |  2|   Mahesh|   java|javascript|test cases|[java, javascript,...|
# +---+---------+-------+---------+----------+--------------------+

# 4.Array_contains:

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import array_contains
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "Gangadhar", ["python", "sql", "pyspark"]), (2, "Mahesh", ["java", "javascript", "test cases"])]
# columns = ["ID", "Name", "skills"]
# df = spark.createDataFrame(data, columns)
# df1 = df.withColumn("has_python_skill", array_contains(df.skills, "python"))
# df1.show(truncate=False)
# ---+---------+--------------------+----------------+
# | ID|     Name|              skills|has_python_skill|
# +---+---------+--------------------+----------------+
# |  1|Gangadhar|[python, sql, pys...|            true|
# |  2|   Mahesh|[java, javascript,...|           false|
# +---+---------+--------------------+----------------+

# ----------------------------------------------------------------------------------------------------------------------
# Map type column:

# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("hair color", df.properties["hair"])
# df2 = df1.withColumn("eye color", df.properties.getItem("eye"))
# df2.show(truncate=False)
# ---------------------------------------------------------------------------------------------------------------------

# map_keys()
# 1.Explode: By using this function  we can get keys and values in seperate columns
# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import explode
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.select("name", "properties", explode(df.properties))
# df1.show(truncate=False)

# 2.  map_keys(): This will display all keys in list format
import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import map_keys
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("keys", map_keys(df.properties))
# df1.show(truncate=False)

# 3. map_values

# import pyspark
# from pyspark.sql.types import StructType, StructField, StringType, MapType
# from pyspark.sql.functions import map_values
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = (("maher", {"hair": "black", "eye": "brown"}), ("rajesh", {"hair": "white", "eye": "blue"}))
# schema = StructType([
#          StructField("name", dataType=StringType()),
#          StructField("properties", MapType(StringType(), StringType()))
#          ])
# df = spark.createDataFrame(data, schema)
# df1 = df.withColumn("values", map_values(df.properties))
# df1.show(truncate=False)


# ----------------------------------------------------------------------------------------------------------------------

# import pyspark
# from pyspark.sql import Row
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetEXAMPLE").getOrCreate()
# row1 = Row(name="DGS", address='medak')
# print(f"Hello my name is {row1[0]} and i am from {row1[1]}")
#
# row2 = Row(name="SKS", address="hyderabad")
# print(f"Hello my name is {row2.name} and i am from {row2.address}")
#
# data = [row1, row2]
# df = spark.createDataFrame(data)
# df.show()
# ---------------------------------------------------------------------------------------------------------------------

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, col, lit
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [("dgs", "m", "medak", 502110), ("trs", "f", "hyderabad", 543212), ("bjp", "", "delhi", 654323)]
# schema = ["name", "gender", "address", "pincode"]
# df = spark.createDataFrame(data, schema)
# df2 = df.select(df.name,
#                 df.address,
#                 df.pincode,
#                 when(df.gender == "m", "male").when(df.gender == "f", "female").otherwise("unknown").alias("gender type"),
#                 when(df.name == "dgs", "gangadhar").alias("actual name").alias("fullname"))
#
#
# df3 = df2.withColumn("country", lit("india"))
# df3.show()

# ---------------------------------------------------------------------------------------------------------------------

# filter() and where():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maher", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000)]
# schema = ["id", "name", "gender", "salary"]
# df = spark.createDataFrame(data, schema)
# df.filter(df.gender == "male").show()
# or
# df.filter("gender == 'male'").show()
# where()
# df.where((df.gender == "male") & (df.id == 1)).show()
#----------------------------------------------------------------------------------------------------------------------

# distinct() and drop duplicates():

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maheer", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000), (3, "asi", "female", 4000)]
# schema = ["id", "name", "gender", "salary"]
# df = spark.createDataFrame(data, schema)
# df1 = df.distinct()
# df1.show()
# # dropduplicate()
# data = [(1, "maheer", "male", 2000), (2, "wafa", "male", 3000), (3, "asi", "female", 4000), (3, "sai", "female", 4500)]
# schema = ["id", "name", "gender", "salary"]
# df2 = spark.createDataFrame(data, schema)
# df2.dropDuplicates(["id"]).show()
#----------------------------------------------------------------------------------------------------------------------

# orderBy() and sort()

# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [(1, "maheer", "male", 2000, "IT"), (2, "wafa", "male", 3000, "HR"), (3, "asi", "female", 4000, "payroll"), (4, "sarfaraj", " male", 4000, "HR")]
# schema = ["id", "name", "gender", "salary", "DEPT"]
# df = spark.createDataFrame(data, schema)
# df.sort("id").show()
# df.orderBy(df.id.desc()).show()
# df.sort(df.DEPT, df.id).show()
# df.orderBy(df.DEPT.desc(), df.id).show()
# ----------------------------------------------------------------------------------------------------------------------

# union() and union all():
# In pyspark union and union all works as same. They won't delete the duplicate rows from the tables
# import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [(1, "maher", "male", 2000), (2, "wafa", "male", 3000)]
# schema1 = ["id", "name", "gender", "salary"]
# df1 = spark.createDataFrame(data1, schema1)
#
#
# data2 = [(3, "sai", "female", 5000), (4, "ramya", "female", 6000), (2, "wafa", "male", 3000)]
# schema2 = ["id", "name", "gender", "salary"]
# df2 = spark.createDataFrame(data2, schema2)

# newdf = df1.union(df2)
# newdf.show()
#
# newdf1 = df1.unionAll(df2)
# newdf1.show()

# To remove the duplicates use distinct():

# newdf2 = newdf1.distinct()
# newdf2.show()

# ----------------------------------------------------------------------------------------------------------------------
# Joins:

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StringType, StructType, StructField, MapType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", {"eye": "brown", "hair": "black"}), ("wafa", {"eye": "red", "hair": "white"})]
# col1 = StructType([
#        StructField("name", StringType()),
#        StructField("properties", MapType(StringType(), StringType()))
#        ])
# df = spark.createDataFrame(data1, col1)
# df1 = df.withColumn("hair", df.properties["hair"])
# df1.withColumn("eyes", df.properties["eye"]).show(truncate=False)

# ---------------------------------------------------------------------------------------------------------------------
# from_json(): from_json is used to convert json string (dict format) type into map type
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import MapType, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black"}'), ("wafa", '{"eye": "red", "hair": "white"}')]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# map_type_schema = MapType(StringType(), StringType())
#
# df1 = df.withColumn("propsmap", from_json(df.properties, map_type_schema))
# df2 = df1.withColumn("hair", df1.propsmap["hair"])
# df3 = df2.withColumn("eye", df1.propsmap["eye"])
# df3.show(truncate=False)
# df1.printSchema()

# ----------------------------------------------------------------------------------------------------------------------
#  Converting Json string type to Struct type:
# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json
# from pyspark.sql.types import StructType, StructField, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black"}'), ("wafa", '{"eye": "red", "hair": "white"}')]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# struct_type = StructType([
#               StructField("eye", StringType()),
#               StructField("hair", StringType())
#               ])
# df1 = df.withColumn("propstruct", from_json(df.properties, struct_type))
# df2 = df1.withColumn("hair", df1.propstruct.hair)
# df3 = df2.withColumn("eye", df2.propstruct.eye)
# df3.show(truncate=False)
# ----------------------------------------------------------------------------------------------------------------------
# to_json():
# to_json is used to convert Dataframe column Map type or struct type to JSON string

import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import to_json
# from pyspark.sql.types import StructType, StructField, StringType
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maheer", {"eye": "brown", "hair": "black"}), ("wafa", {"eye": "red", "hair": "white"})]
# schema = ["id", "properties"]
# df = spark.createDataFrame(data1, schema)
# df1 = df.withColumn("propstring", to_json(df.properties))
# df1.show(truncate=False)
# df1.printSchema()
# ----------------------------------------------------------------------------------------------------------------------

import pyspark
# json_tuple: this is used to select the elements from json string
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import json_tuple
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data1 = [("maher", '{"eye": "brown", "hair": "black", "skin": "white"}'),
#          ("wafa", '{"eye": "red", "hair": "white", "skin": "black"}')]
# schema = ["name", "properties"]
# df = spark.createDataFrame(data1, schema)
# df.select("name", json_tuple(df.properties, "eye", "hair").alias("eye type", "skin color")).show()

# ---------------------------------------------------------------------------------------------------------------------

# get_json_object:

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import get_json_object
# spark = SparkSession.builder.appName("SparkGetExample").getOrCreate()
# data = [("maher", '{"address": {"city": "hyd", "state": "telangana"}, "gender": "male"}'),
#         ("wafa", '{"address": {"city": "guru gram", "state": "haryana"}, "gender": "female"}')]
#
# columns = ["name", "properties"]
# df = spark.createDataFrame(data, columns)
# df1 = df.select("name", get_json_object(df.properties, "$.gender").alias("gender"))
# df1.show()

# import pyspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# spark = SparkSession.builder.appName("SPARKGetExample").getOrCreate()
# data = [
#     (("James", "", "Smith"), ("Java", "Scala", 123), "OH", "M"),
#     (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
#     (("Julia", "", "Williams"), ["CSharp", "VB", "DBMS"], "OH", "F"),
#     (("Maria", "Anne", "Jones"), ["CSharp", "VB", "JAVA"], "NY", "M"),
#     (("Jen", "Mary", "Brown"), ["CSharp", "VB", "CSHARP"], "NY", "M"),
#     (("Mike", "Mary", "Williams"), ["Python", "VB", 123], "OH", "M")]
#
# schema = StructType([
#          StructField("fullname", StructType([
#              StructField("firstname", StringType()),
#              StructField("middlename", StringType()),
#              StructField("lastname", StringType())
#          ])),
#
#          StructField("languages", StructType([
#              StructField("primary", StringType()),
#              StructField("secondary", StringType()),
#              StructField("extra", StringType())
#          ])),
#
#          StructField("COUNTRY", StringType()),
#          StructField("GENDER", StringType())
#         ])
#
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)
# df.printSchema()






import pyspark
# from pyspark.sql import SparkSession
# spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
# data = [[(1, "python", "java"), (2, "pyspark", "html"), "css", "javascript"]]
# schema = StructType([
#          StructField("languages", StructType([
#                 StructField("id", IntegerType()),
#                 StructField("PRIMARY", StringType()),
#                 StructField("SECONDARY", StringType())
#                 ])),
#          StructField("fundamentals", ArrayType(StringType())),
#          StructField("extras", StringType()),
#          StructField("OTHERS", StringType())
#          ])
#
# df = spark.createDataFrame(data, schema)
# df.show(truncate=False)



