# tasks.py

import luigi
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
import datetime

from luigi.contrib.external_program import ExternalProgramTask


class TaskPriceStatistics(ExternalProgramTask):
    spark_session = luigi.parameter()

    def main(self, spark_session):
        print("HI HI HI HI HI HI HI HI HI HI HI HI")