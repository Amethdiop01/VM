object Utils {
  def file_path_to_df(spark: org.apache.spark.sql.SparkSession, file_path: os.Path): org.apache.spark.sql.DataFrame = {
    val df = spark.read
      .option("header", "true")
      .csv("file:///" + file_path)
    return df
  }

  def dir_path_to_df(spark: org.apache.spark.sql.SparkSession, dir_path: os.Path): org.apache.spark.sql.DataFrame = {
    val files_path = os.list(dir_path)
    var df = file_path_to_df(spark, files_path(0))
    for (file_path <- files_path.tail) {
      df = df.union(file_path_to_df(spark, file_path))
    }
    return df
  }
}
