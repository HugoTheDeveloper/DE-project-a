import os
from pyspark.sql import SparkSession
import pandas as pd


def get_files_extension(path):
    path = os.path.join(path)
    _, extension = os.path.splitext(path)
    return extension


def from_xlsx(session, path):
    path = os.path.join(path)
    df_pandas = pd.read_excel(path)
    return session.createDataFrame(df_pandas)


spark = SparkSession.builder.appName("Example").getOrCreate()


print(pd.read_excel(os.path.join('/home/bogdan/projects/DE-project-a/fixtures/oksm.xlsx')))
# class Extractor:
#     def __init__(self, session):
#         self.session = session
#         self.path = None

#     def get_df_from_file(self, path, **kwargs):
#         ext = get_files_extension(path)
#         self.path = path
#         #check in dict
    
#     def from_csv(self, **kwargs):
#         return self.session.read.csv(self.path, **kwargs)
    
#     def from_json(self, )