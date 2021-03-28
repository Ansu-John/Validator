from pyspark.sql.functions import isnan, when, count, col
import datetime

class ValidatorUtil:


    def __init__(self):
        self.DATE_FORMAT = "%Y-%m-d"

    def null_checker(self, targetRecords, targetColumn):
        null_count = 0
        nullRecords = targetRecords.select([count(when(col(targetColumn).isNull(), targetColumn)).alias(targetColumn) for targetColumn in targetRecords.columns])
        if nullRecords.first()[0] > 0:
            null_count = nullRecords.first()[0]
        return null_count

    def nan_checker(self, targetRecords, targetColumn):
        nan_count = 0
        nanRecords = targetRecords.select([count(when(isnan(targetColumn), targetColumn)).alias(targetColumn) for targetColumn in targetRecords.columns])
        if nanRecords.first()[0] > 0:
            nan_count = nanRecords.first()[0]
        return nan_count

    def count_checker(self, sourceCount,targetCount):
        countFlag = False
        if sourceCount == targetCount:
            countFlag = True
        return countFlag

    def date_checker(self, targetRecords):
        nondateRecords = self.spark.emptyDataFrame
        for row in targetRecords:
            try:
                datetime.datetime.strptime(row, self.DATE_FORMAT)
            except:
                nondateRecords.append(row)
        return nondateRecords

    def similarity_checker(self, sourceRecords, targetRecords):
        similarityFlag = False
        unsimilarRecords = self.spark.emptyDataFrame
        if(self.count_checker(sourceRecords.count(), targetRecords.count())):
            unsimilarRecords = targetRecords.subtract(sourceRecords)
        if(unsimilarRecords.count() == 0):
            similarityFlag = True
        return similarityFlag, unsimilarRecords




