from pyspark.sql.types import *
from util.Validator import ValidatorUtil


class Data:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger
        self.validator = ValidatorUtil()

    def validate(self):
        dschema = StructType([StructField("count", IntegerType())])
        targetRecords = self.spark.read.csv('tables/resources/targetData.csv', header = True, schema = dschema)
        sourceRecords = self.spark.read.csv('tables/resources/sourceData.csv', header = True, schema = dschema)
        if self.validator.count_checker(sourceRecords.first()[0],targetRecords.first()[0]):
            print("Count of records in source and target are same and is equal to " + str(targetRecords.first()[0]))
        else:
            print("Count of records in source = " + str(sourceRecords.first()[0]) + " and in target = " + str(targetRecords.first()[0]))
