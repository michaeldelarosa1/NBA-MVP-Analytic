var combined_df = df19.union(df20).union(df21).union(df22)

// convert all numerical columns to floats
val numericalColumns = Seq("AGE", "GP", "W", "L", "MIN", "OFFRTG", "DEFRTG", "NETRTG", "AST%", "AST/TO", "AST RATIO", "OREB%", "DREB%", "REB%", "TO RATIO", "EFG%", "TS%", "USG%", "PACE", "PIE", "POSS")
numericalColumns.foreach(column => combined_df = combined_df.withColumn(column, col(column).cast(FloatType)))
numericalColumns.foreach(column => df23 = df23.withColumn(column, col(column).cast(FloatType)))

// empty array to store statistics in
val statsResults = ArrayBuffer.empty[(String, Double, Double, String, Double)]

// text formatting: conveting String columns "Player" and "TEAM" to lowercase
val post_lowercase_df_train = combined_df.withColumn("Player", lower(col("Player"))).withColumn("TEAM", lower(col("TEAM")))

// binary columns: Winner (1 if played in more wins than losses, 0 if not)
// change column name "Player" to "PLAYER" for continuity and dropped the "INDEX" column
val temp_df_train = post_lowercase_df_train.withColumnRenamed("Player", "PLAYER").drop("INDEX").withColumn("WINNER", when(col("W") > col("L"), 1).otherwise(0)).withColumn("WIN%", round(when(col("GP") =!= 0, col("W") / col("GP")).otherwise(lit(0.0)), 4))
val column_order = Array("PLAYER", "TEAM", "AGE", "GP", "W", "L", "WIN%", "WINNER", "MIN", "OFFRTG", "DEFRTG", "NETRTG", "AST%", "AST/TO", "AST RATIO", "OREB%", "DREB%", "REB%", "TO RATIO", "EFG%", "TS%", "USG%", "PACE", "PIE", "POSS")
val final_df_train = temp_df_train.select(column_order.map(col): _*)

final_df_train.write.option("header", "true").mode("overwrite").csv("cleaned_data_train1.csv")

// FOR THE TESTING DATASET
// text formatting: conveting String columns "Player" and "TEAM" to lowercase
val post_lowercase_df_test = df23.withColumn("Player", lower(col("Player"))).withColumn("TEAM", lower(col("TEAM")))

// binary columns: Winner (1 if played in more wins than losses, 0 if not)
// change column name "Player" to "PLAYER" for continuity and dropped the "INDEX" column
val temp_df_test = post_lowercase_df_test.withColumnRenamed("Player", "PLAYER").drop("INDEX").withColumn("WINNER", when(col("W") > col("L"), 1).otherwise(0)).withColumn("WIN%", round(when(col("GP") =!= 0, col("W") / col("GP")).otherwise(lit(0.0)), 4))
val column_order_test = Array("PLAYER", "TEAM", "AGE", "GP", "W", "L", "WIN%", "WINNER", "MIN", "OFFRTG", "DEFRTG", "NETRTG", "AST%", "AST/TO", "AST RATIO", "OREB%", "DREB%", "REB%", "TO RATIO", "EFG%", "TS%", "USG%", "PACE", "PIE", "POSS")
val final_df_test = temp_df_test.select(column_order.map(col): _*)

final_df_test.write.option("header", "true").mode("overwrite").csv("cleaned_data_test1.csv")