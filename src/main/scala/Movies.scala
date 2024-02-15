import org.apache.spark.sql.SparkSession
import Utils._

object Movies {

  val spark = SparkSession.builder()
      .appName("movies")
      .master("local[*]")
      .getOrCreate()

  val dir_path = os.Path(os.pwd + "/data")

  val df = dir_path_to_df(spark, dir_path)

  def main(args: Array[String]) {
    df.show(3)
  }
}
