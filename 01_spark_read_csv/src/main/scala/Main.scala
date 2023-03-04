import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  StructType,
  StructField,
  StringType,
  IntegerType
}
import org.apache.spark.sql.DataFrame

object Main {

  def spark = {
    val spark = SparkSession
      .builder()
      .appName("SparkReadCSV")
      .master("local[*]")
      .getOrCreate()

    // Set log level to ERROR
    spark.sparkContext.setLogLevel("ERROR")

    // Shutdown hook to close SparkSession nicely
    sys.addShutdownHook(spark.stop())
    spark
  }

  def schema = StructType(
    Array(
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("Occupation", StringType, true)
    )
  )

  def main(args: Array[String]): Unit = {

    // add corrupt record column to schema
    val schemaWithCorruptedColumn =
      schema.add(StructField("_corrupt_record", StringType, true))

    // read csv file with schema
    val df = spark.read
      .option("header", "true")
      .schema(schemaWithCorruptedColumn)
      // mode PERMISSIVE: nulls for missing values
      .option("mode", "PERMISSIVE")
      .option("sep", ",")
      .option("quote", "\"")
      .option("escape", "\\")
      .option("ignoreLeadingWhiteSpace", "true")
      // corrupt column name
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .csv("src/main/resources/demo1/")

    // show all records
    df.show(false)

    // display error records
    displayErrorRecords(df, hasHeader = true)

  }

  /** This function takes in a dataframe and prints the corrupt records to the
    * console.
    *
    * @param df
    *   The dataframe to be checked for corrupt records
    * @param hasHeader
    *   A boolean value indicating whether the dataframe has a header or not
    *
    * @return
    *   A dataframe containing the corrupt records
    */

  def displayErrorRecords(df: DataFrame, hasHeader: Boolean = true) = {
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window

    val columnNameOfCorruptRecord = "_corrupt_record"
    val addLine = hasHeader match {
      case true  => 1
      case false => 0
    }

    val dfMalformed =
      df.withColumn("_globalnumrow", monotonically_increasing_id())
        .withColumn("_filenamePath", input_file_name())
        .withColumn(
          "_numrow",
          row_number()
            .over(
              Window
                .partitionBy("_filenamePath")
                .orderBy("_globalnumrow")
            ) + addLine
        )
        .drop("_globalnumrow")
        .filter(col(columnNameOfCorruptRecord).isNotNull)

    dfMalformed.show(false)
  }

}
