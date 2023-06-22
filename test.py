import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
spark = SparkSession.builder.appName("SparkByExample").getOrCreate()
data = [(1, ("Gangadhar", "sharma"), 5000), (2, ("Mahesh", "pilli"), 5000)]
Structname = StructType([
                         StructField(name="firstname", dataType=StringType()),
                         StructField(name="lastname", dataType=StringType())
             ])
schema = StructType([
                    StructField(name="id", dataType=IntegerType()),
                    StructField(name="name", dataType=Structname),
                    StructField(name="salary", dataType=IntegerType())
                  ])

df = spark.createDataFrame(data, schema)
df.show()