from pyspark.sql.functions import *

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(fld, fmt))