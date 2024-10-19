import luigi, os
from luigi.contrib.hdfs import HdfsTarget
from luigi.contrib.spark import PySparkTask
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import FloatType, IntegerType

class TaskPriceStat(PySparkTask):
    user = "d.arhipova"

    def output(self):
        return HdfsTarget('/user/{}/task3/price_stat/'.format(self.user))

    def run(self):
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        sourcePriceDF = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv("/home/{}/dz4/data3/rosstat/price".format(self.user))
        )

        sourcePriceDF = sourcePriceDF.withColumn("_c2", sf.regexp_replace("_c2", ",", "."))

        sourcePriceDF = (
            sourcePriceDF
            .select(
                sf.col("_c0").cast(IntegerType()).alias("city_id"),
                sf.col("_c1").cast(IntegerType()).alias("product_id"),
                sf.round(sf.col("_c2").cast(FloatType()), 2).alias("price")
            )
        )  
        
        sourceCitiesDF = spark.read.table("user_d_arhipova.cities")
        sourceProductsDF = spark.read.table("user_d_arhipova.products")
        sourceProductsForStatDF = spark.read.table("user_d_arhipova.products_for_stat")
        
        joined_df = (
            sourcePriceDF
            .join(sourceCitiesDF, on="city_id")
            .join(sourceProductsDF, on="product_id")
            .join(sourceProductsForStatDF, on="product_id")
        )

        price_stat = joined_df.groupBy("product_id").agg(
            sf.min("price").alias("min_price"),
            sf.max("price").alias("max_price"),
            sf.round(sf.avg("price"), 2).alias("price_avg")
        )

        price_stat = price_stat.orderBy("product_id")         

        price_stat.write.option("header", "true").option("sep", ";").mode("overwrite").csv(self.output().path)

        spark.stop()


class TaskOkDem(PySparkTask):

    user = "d.arhipova"

    def output(self):
        return HdfsTarget('/user/{}/task3/ok_dem/'.format(self.user))
    
    def run(self):
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        sourceRSCityDF = spark.read.table("user_d_arhipova.rs_city")
        sourceCitiesDF = spark.read.table("user_d_arhipova.cities")
        sourceCoreDemographyDF = spark.read.table("user_d_arhipova.core_demography")
        ok_dem = sourceCoreDemographyDF.join(sourceRSCityDF, on="id_location").join(sourceCitiesDF, on="city_id", how='inner')
        ok_dem = ok_dem.drop("id_location", "create_date", "id_country", "login_region")

        sourcePriceDF = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv("/home/{}/dz4/data3/rosstat/price".format(self.user))
        )

        sourcePriceDF = sourcePriceDF.withColumn("_c2", sf.regexp_replace("_c2", ",", "."))

        sourcePriceDF = (
            sourcePriceDF
            .select(
                sf.col("_c0").cast(IntegerType()).alias("city_id"),
                sf.col("_c1").cast(IntegerType()).alias("product_id"),
                sf.round(sf.col("_c2").cast(FloatType()), 2).alias("price")
            )
        )  

        price_avg = sourcePriceDF.groupBy("product_id").agg(
            sf.mean("price").alias("price_avg")
        )        

        high_price_cities = sourcePriceDF.join(price_avg.select(["product_id","price_avg"]), on = "product_id").filter(sf.col("price") > sf.col("price_avg"))
        high_price_cities = high_price_cities.select("city_id").distinct()

        ok_dem = ok_dem.join(high_price_cities, on = "city_id")

        ok_dem = ok_dem.groupBy("city_name").agg(
        sf.count("id").alias("user_cnt"),
        sf.round(sf.avg(sf.datediff(sf.to_date(sf.lit("2023-03-01")), sf.to_date(sf.expr("timestamp(birth_date * 24*60*60)")))/365.25), 2).alias("avg_age"),
        sf.sum(sf.when(ok_dem.gender == 1, 1).otherwise(0)).alias("men_cnt"),
        sf.sum(sf.when(ok_dem.gender == 2, 1).otherwise(0)).alias("women_cnt"),
        sf.round(sf.sum(sf.when(ok_dem.gender == 1, 1).otherwise(0)) / sf.count("id"), 2).alias("men_share"),
        sf.round(sf.sum(sf.when(ok_dem.gender == 2, 1).otherwise(0)) / sf.count("id"), 2).alias("women_share")
        ).orderBy("user_cnt", ascending=False)

        ok_dem.write.option("header", "true").option("sep", ";").mode("overwrite").csv(self.output().path)

        spark.stop()


class TaskProductStat(PySparkTask):

    user = "d.arhipova"

    def output(self):
        return HdfsTarget('/user/{}/task3/product_stat/'.format(self.user))

    
    def run(self):
        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        sourceCitiesDF = spark.read.table("user_d_arhipova.cities")
        sourceProductsDF = spark.read.table("user_d_arhipova.products")

        sourcePriceDF = (
            spark.read
                .option("header", "false")
                .option("sep", ";")
                .csv("/home/{}/dz4/data3/rosstat/price".format(self.user))
        )

        sourcePriceDF = sourcePriceDF.withColumn("_c2", sf.regexp_replace("_c2", ",", "."))

        sourcePriceDF = (
            sourcePriceDF
            .select(
                sf.col("_c0").cast(IntegerType()).alias("city_id"),
                sf.col("_c1").cast(IntegerType()).alias("product_id"),
                sf.round(sf.col("_c2").cast(FloatType()), 2).alias("price")
            )
        )  

        selected_cities = sourceCitiesDF.filter(sf.col("city_name").isin(["Симферополь", "Новороссийск", "Тихвин", "Казань"]))
        all_cities_price = sourcePriceDF.join(selected_cities, "city_id").join(sourceProductsDF, "product_id")

        product_stat = (
            all_cities_price
            .join(selected_cities, on = "city_name", how="inner")
            .select(
                sf.col("product_name"),
                sf.col("price"),
                sf.col("city_name")
            )
        ) 

        min_max_city_prices = product_stat.groupBy("city_name").agg(
            sf.max("price").alias("max_price"),
            sf.min("price").alias("min_price")
        )
        
        min_price_products = (
            product_stat
            .join(min_max_city_prices, on = "city_name" , how = "inner")
            .select(
                sf.col("city_name"),
                sf.col("min_price"),
                sf.col("product_name"),
            )
            .where(
                product_stat.price == min_max_city_prices.min_price
            )
        ).withColumnRenamed("product_name", "min_priced_product_name")     

        max_price_products = (
            product_stat
            .join(min_max_city_prices, on = "city_name" , how = "inner")
            .select(
                sf.col("city_name"),
                sf.col("max_price"),
                sf.col("product_name"),
            )
            .where(
                product_stat.price == min_max_city_prices.max_price
            )
        ).withColumnRenamed("product_name", "max_priced_product_name")     

        product_stat = min_price_products.join(max_price_products, on = "city_name", how="inner").withColumn("price_diff", sf.col("max_price") - sf.col("min_price"))

        product_stat.write.option("header", "true").option("sep", ";").mode("overwrite").csv(self.output().path)

        spark.stop()


class Pipeline(luigi.WrapperTask):
    user = "d.arhipova"

    def output(self):
        return HdfsTarget('/user/{}/task3/product_stat/'.format(self.user))

    def requires(self):
        return [TaskPriceStat(), TaskOkDem(), TaskProductStat()]



if __name__ == '__main__':
    luigi.build([Pipeline()], local_scheduler=True)