'''
Created on 26-Feb-2019

@author: impadmin
'''
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *

def player_example(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.option("multiline", "true").json("../../resources/player.json")
   
    print(df.count())
     # Displays the content of the DataFrame to stdout
    df.select("ClubCountry").show()
    df.groupBy("ClubCountry").count().show()
    
    # Print the schema in a tree format
    df.printSchema()

def basic_df_example(spark):
    # $example on:create_df$
    # spark is an existing SparkSession
    df = spark.read.json("../../resources/people.json")
   
    print(df.count())
     # Displays the content of the DataFrame to stdout
    df.show()
    
    # Print the schema in a tree format
    df.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # Select only the "name" column
    df.select("name").show()

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()

    # Select people older than 21
    df.filter(df['age'] > 21).show()

    # Count people by age
    df.groupBy("age").count().show()
    # +----+-----+
    # |  30|    1|
    # |  01|    1|
    # |  38|    1|
    # +----+-----+
    # $example off:untyped_ops$

    # $example on:run_sql$
    # Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    sqlDF = spark.sql("SELECT * FROM people")
    sqlDF.show()

    # Register the DataFrame as a global temporary view
    df.createGlobalTempView("people")

    # Global temporary view is tied to a system preserved database `global_temp`
    spark.sql("SELECT * FROM global_temp.people").show()

    # Global temporary view is cross-session
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
    
if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # $example off:init_session$

    basic_df_example(spark)
    player_example(spark)

    '''schema_inference_example(spark)
    programmatic_schema_example(spark)'''

    spark.stop()