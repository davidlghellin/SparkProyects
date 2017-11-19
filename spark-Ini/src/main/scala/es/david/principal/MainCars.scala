package es.david.principal

import es.david.modelo.Cars
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object MainCars extends App {
  val outputDir = "/home/wizord/datahack/spark2"

  val file="/home/wizord/workspaceScala/spark-Ini/src/main/resources/cars.csv"

  val conf = new SparkConf()

  val sc = new SparkContext(conf)

  val carsSource = sc.textFile(file)


  val total = carsSource.count()
  println("Tenemos un total de: " + total)


  val spark: SparkSession = SparkSession
    .builder
    .getOrCreate

  import spark.implicits._

  // TODO revisar
  //val dataFrame = spark.read.format("CSV").option("header","true").load(file)
  val cars = carsSource.map(s => s.split(",")).map(
    aux => Cars(
      aux(0),
      aux(1).toInt,
      aux(2).toInt
    )
  ).toDF()
  cars.show(5)
}
