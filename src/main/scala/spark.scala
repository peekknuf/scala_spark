import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

    val df = spark.read.format("parquet").load("code/Gengo/prayThisWorks")

    val group_by_year = df.withColumn("year", year(col("Timestamp"))).groupBy("year").agg(avg("Price").alias("avg_price")).orderBy("year")

    group_by_year.write.format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("code/Gengo/scala_spark_results")

    spark.stop()
  }
}

