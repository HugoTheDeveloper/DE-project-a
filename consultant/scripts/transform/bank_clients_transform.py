from pyspark.sql import SparkSession
from pyspark.sql.functions  import *

from dotenv import load_dotenv
import sys
import os
import datetime
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../..')))

from consultant.tools.basic import normalize_path, get_hdfs_url
from consultant.tools.patterns import TITLES_REGEX, DEGREES_REGEX


if __name__ == '__main__':
    load_dotenv()

    ENVIRONMENT = os.getenv('ENVIRONMENT')
    HOME_DIR = os.getenv('HOME_DIR')
    FIXTURE_DIR = os.getenv('FIXTURE_DIR')

    spark = (SparkSession.builder
            .appName("bank_clients_transform")
            .getOrCreate())
    
    path = normalize_path(*[HOME_DIR, FIXTURE_DIR, 'clients.csv'])
    clients = spark.read.csv(path, header=True, inferSchema=True)

    filtered_by_name = clients \
        .withColumn(
            'client_name',
            when(
                size(split(col('client_name'), r"\s+")) >= 3,
                regexp_replace(
                col('client_name'),
                rf"(^({TITLES_REGEX})\s*)|(\s+({DEGREES_REGEX})$)",
                "")
            ) \
            .otherwise(col('client_name'))
    )

    cleaned_phones = filtered_by_name \
        .withColumn('client_phone',
                    regexp_replace(
                        col('client_phone'),
                        rf"[^0-9x]",
                        ""
                    )) \
        .withColumn('client_phone', concat(lit('+') ,col('client_phone')))

    cleaned_address = cleaned_phones \
    .withColumn(
        'client_address',
        regexp_replace(
            col('client_address'),
            rf"[\,]",
            ""
        )
    )

    prepared_clients = cleaned_address \
    .dropDuplicates() \
    .na.drop('all')

    path = [os.getenv('OUTPUT_DIR'), 'bank_clients/']
    hdfs_url = get_hdfs_url('bank_clients', True, *path)

    prepared_clients \
        .write.mode('overwrite') \
        .parquet(hdfs_url)
    
    spark.stop()
