from pyspark.sql import SparkSession
from pyspark.sql import Row

from dotenv import load_dotenv
import sys
import os
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../../..')))
print(os.path.join(os.getcwd(), '../../..'))

from consultant.extractors.base import get_cbr_currency_rate_json
from consultant.tools.basic import get_hdfs_url


if __name__ == '__main__':
    load_dotenv()

    spark = (SparkSession.builder
        .appName("cbr_currency_extract_load")
        .getOrCreate())

    currency_list = list(get_cbr_currency_rate_json()['Valute'].values())
    rows = [Row(**currency) for currency in currency_list]

    raw_currency = spark.createDataFrame(rows)

    path = [os.getenv('INPUT_DIR'), 'cbr/']
    hdfs_url = get_hdfs_url('cbr_currency', True, *path)


    raw_currency \
    .write.mode('overwrite') \
    .json(hdfs_url)

    spark.stop()
