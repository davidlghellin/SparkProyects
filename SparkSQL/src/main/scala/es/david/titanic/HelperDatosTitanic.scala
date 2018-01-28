package es.david.titanic

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

object HelperDatosTitanic {

  def cargaDatos(fichero: String, session: SparkSession): DataFrame = {

    val nullable = true

    val squemaArray = Array(
      StructField("assengerId", IntegerType, nullable),
      StructField("survived", IntegerType, nullable),
      StructField("pclass", IntegerType, nullable),
      StructField("name", StringType, nullable),
      StructField("sex", StringType, nullable),
      StructField("age", FloatType, nullable),
      StructField("sibSp", IntegerType, nullable),
      StructField("parch", IntegerType, nullable),
      StructField("ticket", StringType, nullable),
      StructField("fare", FloatType, nullable),
      StructField("cabin", StringType, nullable),
      StructField("embarked", StringType, nullable)
    )

    val trainSquema = StructType(squemaArray)
    val testSquema = StructType(squemaArray.filter(p => p.name != "Survived"))

    val csvFormat: String = "com.databricks.spark.csv"

    val ds: DataFrame = session.read
      .format(csvFormat)
      .option("header", "true")
      .schema(trainSquema)
      .load(fichero)

    ds
  }


}


