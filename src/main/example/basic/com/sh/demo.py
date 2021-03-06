'''
Created on 26-Feb-2019

@author: impadmin
'''
from pyspark.sql import *



def basic_df_example(spark):
    # Create Example Data - Departments and Employees

# Create the Departments
    department1 = Row(id='123456', name='Computer Science')
    department2 = Row(id='789012', name='Mechanical Engineering')
    department3 = Row(id='345678', name='Theater and Drama')
    department4 = Row(id='901234', name='Indoor Recreation')
    
    # Create the Employees
    Employee = Row("firstName", "lastName", "email", "salary")
    employee1 = Employee('michael', 'armbrust', 'no-reply@berkeley.edu', 100000)
    employee2 = Employee('xiangrui', 'meng', 'no-reply@stanford.edu', 120000)
    employee3 = Employee('matei', None, 'no-reply@waterloo.edu', 140000)
    employee4 = Employee(None, 'wendell', 'no-reply@berkeley.edu', 160000)
    
    # Create the DepartmentWithEmployees instances from Departments and Employees
    departmentWithEmployees1 = Row(department=department1, employees=[employee1, employee2])
    departmentWithEmployees2 = Row(department=department2, employees=[employee3, employee4])
    departmentWithEmployees3 = Row(department=department3, employees=[employee1, employee4])
    departmentWithEmployees4 = Row(department=department4, employees=[employee2, employee3])
    
    print(department1)
    print(employee2)
    print(departmentWithEmployees1.employees[0].email)
    
    #Create DataFrames from a list of the rows
    departmentsWithEmployeesSeq1 = [departmentWithEmployees1, departmentWithEmployees2]
    df1 = spark.createDataFrame(departmentsWithEmployeesSeq1)
    print(df1)
    
    departmentsWithEmployeesSeq2 = [departmentWithEmployees3, departmentWithEmployees4]
    df2 = spark.createDataFrame(departmentsWithEmployeesSeq2)

    print(df2)
    
    #Union two DataFrames
    unionDF = df1.unionAll(df2)
    print(unionDF)
    
    #Write the unioned DataFrame to a Parquet file
    #unionDF.write.parquet("/tmp/databricks-df-example.parquet")


    
if __name__ == "__main__":
    # $example on:init_session$
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    basic_df_example(spark)

    spark.stop()