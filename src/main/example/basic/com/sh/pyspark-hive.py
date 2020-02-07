'''
Created on 26-Feb-2019

@author: impadmin
'''
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import col, asc
from pydoc import describe

def hive_sql_df_example(session):
    call_center = session.sql("select * from call_center").alias("c1")
    f(call_center)
    call_center.show()
    call_center.printSchema()
    print(call_center.count())
    
    call_center_dept = session.sql("select * from call_center_dept").alias("c2")
    call_center_dept.show()
    print(call_center_dept.count())
    
    inner_join = call_center.join(call_center_dept, call_center.cc_call_center_sk == call_center_dept.cc_call_center_sk) \
    .select(call_center_dept.cc_call_center_sk, call_center_dept.cc_call_center_id, call_center_dept.cc_class)
    print(inner_join.count())
    inner_join.show()
    
    filter = inner_join.filter(col("cc_class") == "medium")
    filter.show()
    
    sort = filter.sort(col("cc_call_center_sk").asc())
    #.sort(desc("cc_call_center_sk"))
    sort.show()
    call_center.createOrReplaceTempView("table1")
    call_center_df = session.table("table1")
    #session.registerDataFrameAsTable(call_center_dept, "table1")
    #print("table1" in session.tableNames())
    
    #not in 
    #df_filtered = df.filter(~df["column_name"].isin([1, 2, 3]))
    
    #array = [1, 2, 3]
    #dataframe.filter(dataframe.column.isin(*array) == False)
    
    #df_result = df[df.column_name.isin([1, 2, 3]) == False]
    
    print(sorted(call_center_df.collect()) == sorted(call_center_dept.collect()))
    
    if sorted(call_center.collect()) == sorted(call_center_dept.collect()):  
        print("Data match")
    else:
        print("Data does not match")
    
    print("orderBy...")
    order_by =  call_center.orderBy("cc_rec_start_date").show()
        
    print("groupBy...")
    call_center.groupBy("cc_class").count().orderBy("cc_class").show()
    
    '''
    Computes statistics for numeric columns.
    This include count, mean, stddev, min, and max. If no columns are given, this function computes statistics for all numerical columns.
    '''
    print("describe...")
    call_center.describe().show()
    call_center.describe(['cc_call_center_sk', 'cc_rec_start_date', 'cc_closed_date_sk', 'cc_tax_percentage']).show()
    
    print("-------------------------------------------------------------------------------------------------------------")
    
    raw_rdd = session.range(1, 7, 2).collect()
    '''
    Parameters:    
            start – the start value
            end – the end value (exclusive)
            step – the incremental step (default: 1)
            numPartitions – the number of partitions of the DataFrame
    Returns:    
        DataFrame
        
    If only one argument is specified, it will be used as the end value.    
    '''
    print(raw_rdd)
    #print(sqlContext.getConf("spark.sql.shuffle.partitions"))   
        
    cube = call_center.cube("cc_class", call_center.cc_manager).count()
    cube.show()
    cube2 = call_center.cube("cc_class").count()
    cube2.show()
    
    print(call_center.distinct().count())
    
    drop_df = call_center.drop("cc_open_date_sk")
    drop_df.show()
    
    print("dropDuplicates...")
    call_center.dropDuplicates().show()
    
    print("dropna...")
    call_center.dropna().show()
    
    print("explain...")
    call_center.explain()
    '''
    Parameters:    extended – boolean, default False. If False, prints only the physical plan.
    '''
    call_center.explain(True)
    
    print("fillna...")
    call_center.na.fill({'cc_closed_date_sk': 'unknown'}).show()
    
    #print("foreach...")
    #call_center.foreach(f)
    
    print("intersect")
    intersect = call_center.intersect(call_center_dept)
    intersect.show()
    
    print("limit")
    limit = call_center.limit(1).collect()
    print(limit(0))
    
def f(call_center):
    print('start : '+call_center.cc_class)  
          
if __name__ == "__main__":
    session = SparkSession \
        .builder \
        .appName("Python Spark Hive example") \
        .config("hive.metastore.uris", "thrift://192.168.218.63:9083") \
        .enableHiveSupport() \
        .getOrCreate();
    print("start...")
    hive_sql_df_example(session)
    print("end...")
    session.stop()