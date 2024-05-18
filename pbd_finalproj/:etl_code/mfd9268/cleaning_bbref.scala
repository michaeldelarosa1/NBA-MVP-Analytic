// First we load the datasets from 2019-2022 and concatenate them
// turning this process into a function to generate one block of training data
// Need to define functions to help with cleaning 
val removeAccents = udf((s: String) => {
  Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll("\\p{InCombiningDiacriticalMarks}+", "")
})

// Function to apply the UDF to the "Player" column of a DataFrame
def removeAccentsFromPlayerColumn(df: DataFrame): DataFrame = {
  df.withColumn("Player", removeAccents($"Player"))
}

def processYearData(year: Int): DataFrame = {
    val filePath = s"${year}_Advanced.csv"
    val pgFilePath = s"${year}_Per_Game.csv"
    val df = spark.read.format("csv").option("header", "true").load(filePath)

    // Cut trailing and leading spaces for ease of Cleaning
    val trimmed_df = df.columns.foldLeft(df) { (currentDF, columnName) =>
        currentDF.schema.fields.find(_.name == columnName) match {
            case Some(field) if field.dataType == StringType =>
            currentDF.withColumn(columnName, trim(col(columnName)))
            case _ =>
            currentDF
        }
        }

    val post_drop_df = trimmed_df.dropDuplicates("Player-additional").drop("_c19").drop("_c24").drop("Player-additional").drop("Rk")

    // Make the correct columns numerical

    post_drop_df.createOrReplaceTempView("data")

    val columnsToConvert = List("Age","G","MP","PER","`TS%`","3PAr","FTr","`ORB%`","`DRB%`","`TRB%`","`AST%`","`STL%`","`BLK%`","`TOV%`","`USG%`","OWS","DWS","WS","`WS/48`","OBPM","DBPM","BPM","VORP")
    val selectExpr = columnsToConvert.map(col => s"CAST($col AS DOUBLE) AS $col").mkString(", ")
    val otherCols = post_drop_df.columns.filterNot(colName => columnsToConvert.contains(colName.replace("`", ""))).map(colName => s"`$colName`").mkString(", ")
    val sqlQuery = s"SELECT Player, Pos, Tm, $selectExpr FROM data"

    val post_convert_df = spark.sql(sqlQuery)

    // Section for Cleaning/processing
    val columnsToConvert_pg = Set("PTS","AST","TRB","STL","BLK")
    val pg_df = spark.read.format("csv").option("header", "true").load(pgFilePath)
    val post_numerical_temp = pg_df.columns.foldLeft(pg_df) { (currentDF, columnName) =>
    if (columnsToConvert_pg.contains(columnName)) {
        currentDF.withColumn(columnName, col(columnName).cast("double"))
    } else {
        currentDF
    }
    }
    val important_cols_pg = post_numerical_temp.dropDuplicates("Player-additional").select("Player","PTS","BLK","STL","AST","TRB")

    // Convert all String columns to lowercase for normalization
    val suffix = year - 2000
    val post_lowercase_df = post_convert_df.withColumn("Player", lower(concat(col("Player"), lit(s" $suffix"))))
    val post_lowercase_pg = important_cols_pg.withColumn("Player", lower(concat(col("Player"), lit(s" $suffix"))))
    val joined_df = post_lowercase_df.join(post_lowercase_pg.select("Player", "PTS", "AST", "TRB", "STL", "BLK"), Seq("Player"), "left_outer")
    
    // Create "is_frontcourt" column
    val final_df_temp = joined_df.withColumn("is_frontcourt", when(col("Pos").contains("sf") || col("Pos").contains("pf") || col("Pos").contains("c"), 1).otherwise(0))
    val final_df = removeAccentsFromPlayerColumn(final_df_temp)
    final_df
}

// Process each year and union all results
val years = 2019 to 2022
val combined_df = years.map(processYearData).reduce(_ union _)

combined_df.coalesce(1).write.option("header", "true").csv("bbref_cleaned_train1.csv")

// Process the testing data
val test_df = processYearData(2023)

test_df.coalesce(1).write.option("header", "true").csv("bbref_cleaned_test1.csv")