def profileData(year: Int): DataFrame = { 
    val filePath = s"${year}_Advanced.csv"
    val pgFilePath = s"${year}_Per_Game.csv"
    val df = spark.read.format("csv").option("header", "true").load(filePath)

    // Trim column names to avoid issues with spaces
    val trimmed_df = df.columns.foldLeft(df) { (currentDF, columnName) =>
        currentDF.withColumnRenamed(columnName, columnName.trim)
    }.na.fill(0)  // Handle missing values by filling zeros or another appropriate value

    // Drop duplicates and unnecessary columns
    val cleaned_df = trimmed_df.dropDuplicates("Player").drop("_c19", "_c24", "Player-additional", "Rk")

    // Cast numeric columns to double
    val numericColumns = List("Age", "G", "MP", "PER", "`TS%`", "3PAr", "FTr", "`ORB%`", "`DRB%`", "`TRB%`", "`AST%`", "`STL%`", "`BLK%`", "`TOV%`", "`USG%`", "OWS", "DWS", "WS", "WS/48", "OBPM", "DBPM", "BPM", "VORP")
    val converted_df = numericColumns.foldLeft(cleaned_df) { (currentDF, colName) =>
        currentDF.withColumn(colName, col(colName).cast("double"))
    }

    // Join with per game stats
    val columnsToConvert_pg = Set("PTS","AST","TRB","STL","BLK")
    val pg_df = spark.read.format("csv").option("header", "true").load(pgFilePath)
    val pg_cleaned_df = columnsToConvert_pg.foldLeft(pg_df) { (currentDF, colName) =>
        currentDF.withColumn(colName, col(colName).cast("double"))
    }
    val final_pg_cleaned_df = pg_cleaned_df.select("Player","PTS","AST","TRB","STL","BLK")
    val joined_df = converted_df.join(final_pg_cleaned_df, Seq("Player"), "left_outer")

    // Calculate statistics
    val results_df = numericColumns.foldLeft(spark.emptyDataFrame) { (resultsDF, colName) =>
        val stats = joined_df.select(
            mean(col(colName)).alias("Mean"),
            expr("percentile_approx(" + colName + ", 0.5)").alias("Median"),
            stddev(col(colName)).alias("StdDev")
        ).withColumn("Field", lit(colName))

        if (resultsDF.columns.isEmpty) stats
        else resultsDF.union(stats)
    }
    println(s"${year} summary:")

    results_df.show()
    results_df
}

val years = 2019 to 2023
val combined_df = years.map(profileData).reduce(_ union _)