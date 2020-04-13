#spark-submit transform.py --configFile conf-dev.txt --query task1.sql

# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 16:25:41 2020

@author: rnandre
"""
import sys
import ConfigParser
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--configFile")
parser.add_argument("--query")
args = parser.parse_args()
print(args)

def configParser():
    global database, targetTable, sqlFile
    configParser = ConfigParser.RawConfigParser() 
    sqlFile = args.query
    #configFilePath = r'/home/centos/dataOps/conf-dev.txt'
    configFilePath = args.configFile
    configParser.read(configFilePath)
    database = configParser.get('configuration','database')
    print("database:",database)    
    targetTable = configParser.get('configuration','target')
    print("target table: ",targetTable)
    
if __name__ == '__main__':
    configParser()
    filename = open(sqlFile,'r')
    sql =''
    for line in filename:
        sql = sql+" "+line
    sparkSession = (SparkSession
                    .builder
                    .appName('pyspark-sql')
                    .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                    .enableHiveSupport()
                    .getOrCreate()
                    )
    #hiveContext 
    sparkSession.sparkContext.setLogLevel('WARN') 
    print(sql)
    #df_load = sparkSession.sql('SELECT * FROM banking.accounts limit 20')
    #sparkSession.sql("create "+database+" if not exists")
    sparkSession.sql("use "+database)
    df_load = sparkSession.sql(sql)
    df_load.show()
    df_load.createOrReplaceTempView("target");
    sparkSession.sql("drop table if exists "+targetTable);
    sparkSession.sql("create table "+targetTable+" as select * from target");
    #df_load.write().mode("overwrite").saveAsTable(database+"."+targetTable);