#spark-submit ingest.py --configFile conf-dev.txt

import sys
import ConfigParser
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, HiveContext, SQLContext
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--configFile")
args = parser.parse_args()

def configParser():
    global database, ddl_file
    configParser = ConfigParser.RawConfigParser()   
    #configFilePath = r'/home/centos/dataOps/conf-dev.txt'
    #configFilePath = sys.argv[1]
    configFilePath = args.configFile
    configParser.read(configFilePath)
    database = configParser.get('configuration','database')
    print("database:",database)    
    ddl_file = configParser.get('configuration','DDLScript_raw')
    print("DDL Script: ",ddl_file)
    
if __name__ == '__main__':
    configParser()
    #ddl_file='banking_DDL.txt'
    ddl_final = []
    ddl = open(ddl_file,'r')
    ddl_tmp =''
    for line in ddl:
        sql = line
        if sql.lower().find("create") != -1:
            #ddl_final.append(ddl_tmp)
            ddl_tmp = ''
            ddl_tmp = sql
        else:
            if sql.lower().find("load") != -1:
                ddl_final.append(ddl_tmp)
                ddl_final.append(sql)
            else:             
                ddl_tmp = ddl_tmp+" "+ sql  
    #print(ddl_final[1])
#    ddl_final.append(ddl_tmp)
    sparkSession = (SparkSession
                    .builder
                    .appName('pyspark-sql')
                    .config("hive.metastore.uris", "thrift://localhost:9083", conf=SparkConf())
                    .enableHiveSupport()
                    .getOrCreate()
                    )
    sparkSession.sparkContext.setLogLevel('WARN') 
    sc = sparkSession.sparkContext
    sqlContext = SQLContext(sc)
    sqlContext.sql("drop database if exists "+database+" cascade")
    sqlContext.sql("create database "+database)
    sqlContext.sql("use "+database)
    for i in range(len(ddl_final)):
        print(ddl_final[i])
        sql = sqlContext.sql(ddl_final[i])
        #LOAD DATA LOCAL INPATH '/home/centos/dataOps/banking_data/completedacct.csv' OVERWRITE INTO TABLE accounts;

    