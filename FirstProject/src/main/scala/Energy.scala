import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Energy {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Energy")
      .getOrCreate()

    val temp = spark.read.option("header", "true").csv("/chemin/vers/votre/fichier.csv")

    val sub_temp = temp
      .filter(col("code_insee_region") === 24 || col("code_insee_region") === 28)

    val energy = List("Eolien (MW)", "Solaire (MW)", "Hydraulique (MW)", "Bioénergies (MW)")
    val other = List("Date", "Consommation (MW)", "Code INSEE région")

    val sub_elec = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv("energie.csv")
      .select((other ++ energy).map(col(_)): _*)
      .filter(
        col("Code INSEE région") === 24 ||
        col("Code INSEE région") === 28
      )
      .na.fill(0.0)
      .withColumn("Renewable (MW)", array(energy.map(col(_)): _*).reduce((x, y) => x + y))
      .drop(energy: _*)

    sub_elec.show()

    spark.stop()
  }
}
