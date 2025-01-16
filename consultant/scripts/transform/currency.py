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
from consultant.tools.basic import get_jdbc_url_for_gp, get_properies_for_gp

if __name__ == '__main__':
    load_dotenv()

    spark = (SparkSession.builder
         .appName("cbr_currency_etl")
         .getOrCreate())
    
    currency_list = list(get_cbr_currency_rate_json()['Valute'].values())

    new_schema = StructType([
        StructField('code_iso', StringType(), False),
        StructField('id', StringType(), False),
        StructField('name', StringType(), False),
        StructField('nominal', IntegerType(), False),
        StructField('num_code', StringType(), False),
        StructField('rate', FloatType(), False)
    ])

    raw_currency = spark.createDataFrame(currency_list)
    
    prepared_currency = raw_currency \
    .withColumn('unit_rate', (col('Value') / col('Nominal').cast('float'))) \
    .withColumn('on_date', lit(datetime.date.today().cast('date'))) \
    .select(['code_iso', 'nominal', 'rate', 'unit_rate', 'on_date'])

    # Writing

    print(get_properies_for_gp())
    print(get_jdbc_url_for_gp())

    prepared_currency.write.jdbc(
        url=get_jdbc_url_for_gp(),
        table='b_kustov.currency_rate',
        properties=get_properies_for_gp(),
        mode='append'
    )

    spark.stop()