import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, ArrayType, TimestampType}

object Streaming {
    def main(args: Array[String]) {

        val conf = new SparkConf()
        .setAppName("SparkStreaming")
        .set("spark.streaming.stopGracefullyOnShutdown", "true")

        val spark = SparkSession.builder()
        .master("local[2]")
        .config(conf)
        .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR") // Pour ne pas avoir des informations inutiles dans le terminal lors de l'exécution.

        // Définir le schéma des données
        val schema = new StructType()
            .add("id", IntegerType, true)
            .add("order_time", TimestampType)
            .add("shop", StringType, true)
            .add("name", StringType, true)
            .add("phoneNumber", StringType, true)
            .add("address", StringType, true)
            .add("pizzas", new StructType()
                .add("pizzaName", StringType)
                .add("price", IntegerType)
            )

        // Lecture en continu avec le schéma défini
        val df = spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 2)
            .json("files/")

        // Vous pouvez maintenant utiliser le DataFrame df dans votre logique de traitement en continu.

        // Exemple de sortie en continu
        df.writeStream
            .outputMode("append")
            .format("console")
            .start()
            .awaitTermination()

    }
}
