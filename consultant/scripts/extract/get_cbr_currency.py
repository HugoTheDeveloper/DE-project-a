from pyspark.sql import SparkSession
from pyspark.sql.functions  import *
from pyspark.sql.types import StructField, StructType, StructType, IntegerType, FloatType, DateType, StringType

from dotenv import load_dotenv
import sys
import os
import datetime
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

from consultant.extractors.base import get_cbr_currency_rate_json
from consultant.tools.basic import get_hdfs_url


# Troubles with writing in hdfs. Common open writes to local fs

if __name__ == '__main__':
    load_dotenv()

    spark = (SparkSession.builder
         .appName("cbr_currency_extract_load")
         .getOrCreate())
    
    currency_list = list(get_cbr_currency_rate_json()['Valute'].values())
    
    raw_currency = spark.createDataFrame(currency_list)
    

   