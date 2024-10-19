# main.py
import luigi

from luigi.util import requires
from tasks import TaskPriceStatistics
from pyspark.sql import SparkSession

#@requires(TaskPriceStatistics)
def main():
    spark = spark = SparkSession.builder.getOrCreate()
    
    # Передача spark-сессии параметризованной задаче
    luigi.build([TaskPriceStatistics(spark_session=spark)], local_scheduler=True)

if __name__ == "__main__":
    luigi.run()
