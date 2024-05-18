import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.upper
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.evaluation.RegressionEvaluator
import java.text.Normalizer
import org.apache.spark.sql.types.{FloatType, StringType, DoubleType}

val spark = SparkSession.builder().appName("NBA Stats Analysis").getOrCreate()

// training data
var df19_temp = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/nba.com_19_20.csv")
var df20_temp = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/nba.com_20_21.csv")
var df21_temp = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/nba.com_21_22.csv")
var df22_temp = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/nba.com_22_23.csv")
var df19 = df19_temp.withColumn("PLAYER", concat($"PLAYER", lit(" 19")))
var df20 = df20_temp.withColumn("PLAYER", concat($"PLAYER", lit(" 20")))
var df21 = df21_temp.withColumn("PLAYER", concat($"PLAYER", lit(" 21")))
var df22 = df22_temp.withColumn("PLAYER", concat($"PLAYER", lit(" 22")))

// testing data
var df23_temp = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/nba.com_23_24.csv")
var df23 = df23_temp.withColumn("PLAYER", concat($"PLAYER", lit(" 23")))