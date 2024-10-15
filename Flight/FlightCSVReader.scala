import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, concat, lpad, when, date_add, date_format}

object FlightCSVReader {

  def main(args: Array[String]): Unit = {
    suppressLogs()

    // Check if the directory argument was provided
    if (args.length < 1) {
      println("Usage: FlightCSVReader <directory>")
      System.exit(1)
    }

    val flightDir = new File(args(0))

    // Initialize Spark session
    val spark = createSparkSession()

    // Validate directory and CSV files
    validateDirectory(flightDir)

    val csvFiles = getCSVFiles(flightDir)

    // Load and process the CSV files
    val (totalFiles, totalRows) = loadAndProcessFiles(csvFiles, spark)

    println(s"Nombre total de fichiers lus : $totalFiles")
    println(s"Nombre total de lignes chargées : $totalRows")

    val concatenatedDF = loadAndConcatCSVFiles(csvFiles, spark)

    // Show schema and filtered results
    val df_processed = showSchemaAndFilter(concatenatedDF, spark)

    //compute the SCHEDULED ARRIVAL TIME
    val df_withscheduledarrival = addScheduledArrTimeColumn(df_processed)

    df_withscheduledarrival.show(5)

    // Stop Spark session
    spark.stop()
  }

  /** Suppresses verbose logs for better readability */
  def suppressLogs(): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
  }

  /** Creates a new Spark session */
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName("FlightCSVReader")
      .master("local[*]")  // "local[*]" to use all CPU cores
      .getOrCreate()
  }

  /** Validates if the given directory exists and is valid */
  def validateDirectory(flightDir: File): Unit = {
    if (!flightDir.exists() || !flightDir.isDirectory) {
      println(s"Le répertoire flight n'existe pas : ${flightDir.getAbsolutePath}")
      System.exit(1)
    }
  }

  /** Retrieves all CSV files from the given directory */
  def getCSVFiles(flightDir: File): Array[File] = {
    val csvFiles = flightDir.listFiles().filter(_.getName.endsWith(".csv"))
    if (csvFiles.isEmpty) {
      println(s"Aucun fichier CSV trouvé dans le répertoire : ${flightDir.getAbsolutePath}")
      System.exit(1)
    }
    csvFiles
  }

  /** Loads and processes the CSV files, returning total files and rows processed */
  def loadAndProcessFiles(csvFiles: Array[File], spark: SparkSession): (Int, Int) = {
    var totalRows = 0
    var totalFiles = 0

    csvFiles.foreach { file =>
      totalFiles += 1
      println(s"Chargement du fichier : ${file.getName}")

      // Load CSV into DataFrame
      val df: DataFrame = spark.read
        .option("header", "true")
        .csv(file.getAbsolutePath)

      // Count rows
      val rowCount = df.count()
      totalRows += rowCount.toInt

      println(s"Nombre de lignes dans ${file.getName}: $rowCount")
    }

    (totalFiles, totalRows)
  }

  /** Drops rows with missing values in the specified columns */
  def dropMissingValues(df: DataFrame, columns: Seq[String]): DataFrame = {
    df.na.drop("any", columns)
  }

  /** Cast specified columns to integer */
  def castColumnsToInt(df: DataFrame, columns: Seq[String]): DataFrame = {
    columns.foldLeft(df)((tempDF, colName) => tempDF.withColumn(colName, col(colName).cast("int")))
  }

  /** Displays schema, prints initial row count, applies filters, and prints the filtered row count */
  def showSchemaAndFilter(df: DataFrame, spark: SparkSession): DataFrame = {

    // Show the schema of the initial DataFrame
    df.printSchema()

    // Print the number of rows before filtering
    val initialRowCount = df.count()
    println(s"Nombre initial de lignes: $initialRowCount")

    // Drop rows with missing values in critical columns
    val nonMissingDF = dropMissingValues(df, Seq("FL_DATE", "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME"))

    val withintDF = castColumnsToInt(nonMissingDF, Seq("OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM",
       "ORIGIN_AIRPORT_ID", "DEST_AIRPORT_ID", "CRS_DEP_TIME", "ARR_DELAY_NEW",
       "CANCELLED", "DIVERTED", "CRS_ELAPSED_TIME", "WEATHER_DELAY",
       "NAS_DELAY"))

    val filledDF = withintDF.na.fill(0, Seq("ARR_DELAY_NEW", "WEATHER_DELAY", "NAS_DELAY"))


    // Apply filters and clean up the data
    val filteredDF: DataFrame = filledDF
      .filter("CANCELLED != 1")
      .filter("DIVERTED != 1")
      .drop("OP_CARRIER_AIRLINE_ID", "OP_CARIIER_FL_NUM", "CANCELLED", "DIVERTED", "_c12")

    // Show the schema after filtering
    filteredDF.printSchema()

    // Print the number of rows after filtering
    val filteredRowCount = filteredDF.count()
    println(s"Nombre de lignes après filtres: $filteredRowCount")

    filteredDF.show(5)
    //return the transformed dataframe
    filteredDF
  }

  /** Function to create SCHEDULED_ARR_TIME in HHMM format */
  def addScheduledArrTimeColumn(df: DataFrame): DataFrame = {
    // Step 1: Convert CRS_DEP_TIME from HHMM to total minutes after midnight
    val dfWithConvertedDepTime = df.withColumn(
      "CRS_DEP_TIME_MIN",
      ((col("CRS_DEP_TIME") / 100).cast("int") * 60 + col("CRS_DEP_TIME").cast("int") % 100)
    )

    // Step 2: Calculate the SCHEDULED_ARR_TIME in minutes
    val dfWithScheduledArrTimeMin = dfWithConvertedDepTime.withColumn(
      "SCHEDULED_ARR_TIME_MIN",
      col("CRS_DEP_TIME_MIN") + col("CRS_ELAPSED_TIME") - col("ARR_DELAY_NEW")
    )

    // Step 3: Convert SCHEDULED_ARR_TIME_MIN back to HHMM format
    val dfWithScheduledArrTime = dfWithScheduledArrTimeMin.withColumn(
      "SCHEDULED_ARR_TIME",
      concat(
        lpad((col("SCHEDULED_ARR_TIME_MIN") / 60).cast("int"), 2, "0"),  // Hours
        lpad((col("SCHEDULED_ARR_TIME_MIN") % 60).cast("int"), 2, "0")   // Minutes
      )
    )

    // Step 4: Adjust FL_DATE if the scheduled arrival time goes to the next day
      val dfWithDateAdjustment = dfWithScheduledArrTime
      .withColumn("FL_DATE_ARRIVAL",
        date_format(
        when(col("SCHEDULED_ARR_TIME_MIN") >= 1440, date_add(col("FL_DATE"), 1))  // Add 1 day if SCHEDULED_ARR_TIME_MIN exceeds 1440 minutes (24 hours)
          .otherwise(col("FL_DATE"))
        , "yyyyMMdd").cast("int")
      )
      .withColumn("FL_DATE", date_format(col("FL_DATE"), "yyyyMMdd").cast("int"))

      

    // Drop intermediate columns used for calculation if needed
    dfWithDateAdjustment.drop("CRS_DEP_TIME_MIN", "SCHEDULED_ARR_TIME_MIN")
  
  }

  def loadAndConcatCSVFiles(csvFiles: Array[File], spark: SparkSession): DataFrame = {
    // Initialize an empty DataFrame to start with
    var finalDF: DataFrame = spark.emptyDataFrame

    // Loop through all CSV files, read and concatenate them
    csvFiles.foreach { file =>
      println(s"Loading and concatenating file: ${file.getName}")
      
      // Load the CSV file into a DataFrame
      val df = spark.read
        .option("header", "true")  // Assuming the CSV files have headers
        .csv(file.getAbsolutePath)
      
      // If the DataFrame is not empty, concatenate it with the previous DataFrame
      if (!finalDF.isEmpty) {
        finalDF = finalDF.union(df)
      } else {
        // If it's the first file, assign it directly to finalDF
        finalDF = df
      }
    }

    // Partition the DataFrame by the TradeDate (or another column)
    val partitionedDF = finalDF.repartition(col("FL_DATE"))

    // Return the final concatenated DataFrame
    partitionedDF
  }


}
