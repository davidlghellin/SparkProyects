package es.david.titanic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


import org.apache.spark.sql.functions._

object MainTitanic {

  val rutaTraining: String = "/home/wizord/Git/SparkProyects/SparkSQL/src/main/resources/titanic/train.csv"
  //val rutaTest: String = ???


  val conf: SparkConf = new SparkConf()
  val sc: SparkContext = new SparkContext()

  val session: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val traininDF: DataFrame = HelperDatosTitanic.cargaDatos(rutaTraining, session)


    // mostramos unos descriptivos de todos, recordar que tendríamos que hacerlo solo de numeros
    traininDF.drop("passsengerId").describe().show()

    // visualizamos los 20 datos primeros para tener una idea
    traininDF.drop("passsengerId").select("*").show()

    // Muchas de las funciones estan en:
    // org.apache.spark.sql.functions._
    traininDF.select(mean("age")).show()

    traininDF.drop("passsengerId").groupBy("sex").avg().show()
  }

}
