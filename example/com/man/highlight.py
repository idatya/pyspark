'''
Created on 26-Sep-2019

@author: impadmin
'''

from pyatspi.utils import clearCache

from pyspark.sql import SparkSession


def hive_sql(session):
    df = session.sql("select * from call_center")
    df.createOrReplaceTempView("table1")
    df2 = session.sql("SELECT cc_call_center_sk AS f1, cc_call_center_id as f2 from table1")
    print(df2.collect())
    
    df3 = session.table("call_center")
    if sorted(df.collect()) == sorted(df3.collect()):
        print("data matched")
    else:
        print("no match")
        
    session.cacheTable("call_center")
    clearCache()
    
    
if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Python Spark Hive example") \
        .config("spark.debug.maxToStringFields", "10000") \
        .config("hive.metastore.uris", "thrift://192.168.218.63:9083") \
        .enableHiveSupport() \
        .getOrCreate();
    print("start...")
    hive_sql(session)
    print("end...")
    session.stop()