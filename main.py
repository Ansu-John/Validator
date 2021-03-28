from pyspark.sql import *

from pyspark.sql.types import *

from util.Logger import Log4j
from tables.Data import Data

def start():
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("Validator") \
        .getOrCreate()

    logger = Log4j(spark)
    try:
        data = Data(spark, logger)
        data.validate()
    except Exception as ex:
        print(ex)




if __name__ == "__main__":
    start()
