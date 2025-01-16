from pyspark.sql import SparkSession
from pyspark.sql.functions  import *

from dotenv import load_dotenv
import sys
import os
import datetime
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

from consultant.tools.basic import normalize_path, get_hdfs_url

if __name__ == '__main__':
    load_dotenv()

    ENVIRONMENT = os.getenv('ENVIRONMENT')
    HOME_DIR = os.getenv('HOME_DIR')
    FIXTURE_DIR = os.getenv('FIXTURE_DIR')

    spark = (SparkSession.builder
            .appName("bank_transactions_transform")
            .getOrCreate())

    # Reading
    path = normalize_path(*[HOME_DIR, FIXTURE_DIR, 'bank_transactions.csv'])

    transactions = spark.read.csv(path, header=True, inferSchema=True)
    
    # Correcting and deduplicating, dropping nulls
    corrected_amount = transactions \
    .withColumn(
        'amount',
        regexp_replace(
            col('amount'),
            r"(\d+\.\d{2})\d",
            r"\1"
        )
    )

    prepared_transactions = corrected_amount \
        .dropDuplicates() \
        .na.drop('all')
    
    # writing

    path = [os.getenv('OUTPUT_DIR'), 'bank_transactions/']
    hdfs_url = get_hdfs_url('bank_transactions', True, *path)

    prepared_transactions \
        .write.mode('append') \
        .parquet(hdfs_url)

    spark.stop()
