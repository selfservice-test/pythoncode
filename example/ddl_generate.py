# spark-submit ddl_generate.py --configFile conf-dev.txt

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
    configFilePath = args.configFile
    configParser.read(configFilePath)
    database = configParser.get('configuration','database')
    print("database:",database)    
    ddl_file = configParser.get('configuration','DDLScript')
    print("ddl script: ",ddl_file)
    
if __name__ == '__main__':
    configParser()
#    ddl_file='banking_DDL.txt'
    ddl_final = []
    ddl = open(ddl_file,'r')
    ddl_tmp =''
    for line in ddl:
        sql = line
        #print(sql)
        if sql.lower().find("create") != -1:          
            ddl_tmp = ''
            #print("create", sql)
            ddl_tmp = sql
            #print(create_tmp)
        else:
            if sql.lower().find("insert") != -1:
                ddl_final.append(ddl_tmp)
                ddl_final.append(sql)
		i = sql.lower().index("from")
		ddl_final.append("TRUNCATE table "+ sql[i+5:])
            else:             
                ddl_tmp = ddl_tmp+" "+ sql 
            #print(ddl_tmp)
    print(ddl_final[0], ddl_final[1],ddl_final[2])
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
    sqlContext.sql("use "+database)
    for i in range(len(ddl_final)):
        sql = sqlContext.sql(ddl_final[i])
        #LOAD DATA LOCAL INPATH '/home/centos/dataOps/banking_data/completedacct.csv' OVERWRITE INTO TABLE accounts;

    