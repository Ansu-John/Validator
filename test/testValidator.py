from datetime import date
from unittest import TestCase
import numpy as np

from pyspark.sql import *
from pyspark.sql.types import *

from util.Validator import ValidatorUtil


class ValidatorTestCase(TestCase):

    @classmethod
    def setUpClass(appinit) -> None:
        appinit.spark = SparkSession.builder \
            .master("local[3]") \
            .appName("Validator") \
            .getOrCreate()

        appinit.myValidator = ValidatorUtil()

        my_schema = StructType([
            StructField("ID", StringType()),
            StructField("EventDate", StringType())])

        my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        null_rows = [Row(None, "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
        nan_rows = [Row(np.nan, "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row(float('nan'), "4/05/2020")]
        appinit.my_df = appinit.spark.createDataFrame(appinit.spark.sparkContext.parallelize(my_rows, 2), my_schema)
        appinit.null_df = appinit.spark.createDataFrame(appinit.spark.sparkContext.parallelize(null_rows, 2), my_schema)
        appinit.nan_df = appinit.spark.createDataFrame(appinit.spark.sparkContext.parallelize(nan_rows, 2), my_schema)

    def test_null_checker_false(self):
        nullRecordCount = self.myValidator.null_checker(self.my_df,"ID")
        print("Number of null records in the target column in test_null_checker_false method = " + str(nullRecordCount))
        self.assertTrue(nullRecordCount == 0, "There are no null records in the target column.")

    def test_null_checker_true(self):
        nullRecordCount = self.myValidator.null_checker(self.null_df,"ID")
        print("Number of null records in the target column in test_null_checker_true method = " + str(nullRecordCount))
        self.assertTrue(nullRecordCount > 0, "There are null records in the target column.")

    def test_nan_checker_false(self):
        nanRecordCount = self.myValidator.nan_checker(self.my_df,"ID")
        print("Number of nan records in the target column in test_nan_checker_false method = " + str(nanRecordCount))
        self.assertTrue(nanRecordCount == 0, "There are no null records in the target column.")

    def test_nan_checker_true(self):
        nanRecordCount = self.myValidator.nan_checker(self.nan_df,"ID")
        print("Number of nan records in the target column in test_nan_checker_true method = " + str(nanRecordCount))
        self.assertTrue(nanRecordCount > 0, "There are null records in the target column.")