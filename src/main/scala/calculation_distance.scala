import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row, functions}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StringType, StructField, DateType, IntegerType, FloatType, StructType, DoubleType}
import org.apache.lucene.util.SloppyMath.haversinMeters
import org.apache.spark.sql.functions._

object calculation_distance {
    def main(args: Array[String]) {
        val conf = new SparkConf()
            .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .set("spark.sql.shuffle.partitions", "1000")
            .set("spark.sql.broadcastTimeout", "1200")
            .set("spark.shuffle.service.enabled", "true")
            .set("spark.executor.cores", "1")
            .set("spark.executor.instances", "2")
            .set("spark.executor.memory", "4G")
            .set("spark.driver.cores", "2")
            .set("spark.driver.memory", "4G")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)
        val spark = SparkSession.builder
            .appName("calculation_distance")
            .master("yarn")
            .config(conf=conf)
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        val newDF = spark.table("point_coordinates")
        def filter_station (data_in: DataFrame) : DataFrame = {
            val data_out: DataFrame = data_in.where(data_in("day") === "9999-12-31").select(
                "point_name",
                "point_latitude",
                "point_longitude")
            return data_out
        }
        def cross_join(data_in: DataFrame) : DataFrame = {
            val left_data: DataFrame = data_in.select(
                $"point_name",
                $"point_latitude",
                $"point_longitude")
            val right_data: DataFrame = data_in.select(
                $"point_name".alias("next_point_name"),
                $"point_latitude".alias("next_point_latitude"),
                $"point_longitude".alias("next_point_longitude"))
            val data_out: DataFrame = left_data.crossJoin(right_data)
            return data_out
        }    
        def distance(a: Double, b: Double, c: Double, d: Double) : Double = {
            var e = haversinMeters(a, b, c, d)
            return e
        }
        val distanceUDF = spark.sqlContext.udf.register("distanceUDF", distance _)
        def distance_column(data_in: DataFrame) : DataFrame = {
            val data_out: DataFrame = data_in.withColumn("distance", round(distanceUDF(col(
                "point_latitude"), col("point_longitude"),col(
                "next_point_latitude"), col("next_point_longitude")), 0))
            return data_out
        }    
        def filter_distance(data_in: DataFrame, dis: Int) : DataFrame = {
            val data_out: DataFrame = data_in.where(data_in("distance") <= dis).select("*")
            return data_out
        }        
        def collect_data(data_in: DataFrame) : DataFrame = {
            val data_out: DataFrame = data_in.select(
                $"point_name",
                $"next_point_name", 
                $"distance",
                lit(current_timestamp()).alias("process_dttm"),
                lit(current_date()).alias("day")
            )
            return data_out
        }
        var data: DataFrame  = filter_station(newDF)
        data = cross_join(data)
        data = distance_column(data)
        data = filter_distance(data, 1500)
        data = collect_data(data)
        data.repartition($"day").write.format("orc").mode(
            "overwrite").insertInto("point_between_distance")
        sc.stop()

    }
}