// Loading the datasets and merging on Player names
var nba_df_train = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/cleaned_data_train.csv")
var nba_df_test = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/cleaned_data_test.csv")
var bbref_df_temp_train = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/bbref_cleaned_train.csv")
val bbref_df_train = bbref_df_temp_train.withColumnRenamed("Player","PLAYER").withColumnRenamed("Age", "DROP").withColumnRenamed("TS%","DROP").withColumnRenamed("AST%","DROP").withColumnRenamed("USG%","DROP")
var bbref_df_temp_test = spark.read.option("header", true).csv("hdfs:///user/mfd9268_nyu_edu/bbref_cleaned_test.csv")
val bbref_df_test = bbref_df_temp_test.withColumnRenamed("Player","PLAYER").withColumnRenamed("Age", "DROP").withColumnRenamed("TS%","DROP").withColumnRenamed("AST%","DROP").withColumnRenamed("USG%","DROP")

// TRAINING DATA

val working_df_train = nba_df_train.join(bbref_df_train, "PLAYER")

val final_df_temp_train = working_df_train.drop("W","L","WINNER","Tm","G","MP","DROP","is_frontcourt")

// Converting the data to use in the regression
val final_df_train = final_df_temp_train.filter($"GP" >= 10)

val columnsNotToConvert = Set("PLAYER","TEAM", "Pos")
// Convert to numerical values 
val post_numerical_temp_train = final_df_train.columns.foldLeft(final_df_train) { (currentDF, columnName) =>
  if (!columnsNotToConvert.contains(columnName)) {
    currentDF.withColumn(columnName, col(columnName).cast("double"))
  } else {
    currentDF
  }
}

val post_numerical_train = post_numerical_temp_train.withColumnRenamed("WS", "label")

// We first tried to predict Win%, but after we decided that it wasn't the best idea since its very team dependent
// val only_numerical = post_numerical.drop(columnsNotToConvert.toSeq: _*).withColumnRenamed("WIN%", "label")

val only_numerical_train = post_numerical_train.drop(columnsNotToConvert.toSeq: _*).drop("OWS").drop("DWS").drop("WS/48").withColumnRenamed("WS", "label")

// Processing data for Linear Regression
val featureColumns_train = only_numerical_train.columns.filter(_ != "label").filterNot(columnsNotToConvert.contains)
val assembler_train = new VectorAssembler().setInputCols(featureColumns_train).setOutputCol("features").setHandleInvalid("skip")
val outputData_train = assembler_train.transform(post_numerical_train).select("PLAYER","Pos","TEAM","features", "label")

outputData_train.show()

// TESTING DATA

val working_df_test = nba_df_test.join(bbref_df_test, "PLAYER")

val final_df_temp_test = working_df_test.drop("W","L","WINNER","Tm","G","MP","DROP","is_frontcourt")

// Converting the data to use in the regression
val final_df_test = final_df_temp_test.filter($"GP" >= 10)

val columnsNotToConvert = Set("PLAYER","TEAM", "Pos")
// Convert to numerical values 
val post_numerical_temp_test = final_df_test.columns.foldLeft(final_df_test) { (currentDF, columnName) =>
  if (!columnsNotToConvert.contains(columnName)) {
    currentDF.withColumn(columnName, col(columnName).cast("double"))
  } else {
    currentDF
  }
}

val post_numerical_test = post_numerical_temp_test.withColumnRenamed("WS", "label")

// We first tried to predict Win%, but after we decided that it wasn't the best idea since its very team dependent
// val only_numerical = post_numerical.drop(columnsNotToConvert.toSeq: _*).withColumnRenamed("WIN%", "label")

val only_numerical_test = post_numerical_test.drop(columnsNotToConvert.toSeq: _*).drop("OWS").drop("DWS").drop("WS/48").withColumnRenamed("WS", "label")

// Processing data for Linear Regression
val featureColumns_test = only_numerical_test.columns.filter(_ != "label").filterNot(columnsNotToConvert.contains)
val assembler_test = new VectorAssembler().setInputCols(featureColumns_test).setOutputCol("features").setHandleInvalid("skip")
val outputData_test = assembler_test.transform(post_numerical_test).select("PLAYER","Pos","TEAM","features", "label")
