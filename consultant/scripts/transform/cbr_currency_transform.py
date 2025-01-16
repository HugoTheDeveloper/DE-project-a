from pyspark.sql import SparkSession
from pyspark.sql.functions  import *

from dotenv import load_dotenv
import sys
import os
import datetime
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

from consultant.tools.basic import get_hdfs_url


if __name__ == '__main__':
    load_dotenv()

    spark = (SparkSession.builder
         .appName("cbr_currency_transform")
         .getOrCreate())
    
    path = [os.getenv('INPUT_DIR'), 'cbr/']
    hdfs_url = get_hdfs_url('cbr_currency', True, *path)

    raw_currency = spark.read.json(hdfs_url)
    
    prep_currency = raw_currency \
    .withColumn('VunitRate', col('Value') / col('Nominal')) \
    .withColumn('ValCursDate', lit(datetime.date.today())) \
    .select(['CharCode', 'Nominal', 'Value', 'VunitRate', 'ValCursDate'])

    prep_currency_new_schema = prep_currency.select(
        col("CharCode").alias("code_iso"),
        col("Nominal").cast("int").alias("nominal"),
        col("Value").cast("float").alias("rate"),
        col("VunitRate").cast("float").alias("unit_rate"),
        col("ValCursDate").cast("date").alias("on_date")
    )

    path = [os.getenv('OUTPUT_DIR'), 'cbr/']
    hdfs_url = get_hdfs_url('cbr_currency', True, *path)


    raw_currency \
    .write.mode('overwrite') \
    .json(hdfs_url)

    spark.stop()