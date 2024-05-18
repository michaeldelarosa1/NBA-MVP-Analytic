// calculate statistics for each column, including multiple nodes if applicable
numericalColumns.foreach { column =>
  val meanValue = BigDecimal(combined_df.agg(mean(col(column))).first().getDouble(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  val median = BigDecimal(combined_df.stat.approxQuantile(column, Array(0.5), 0.0)(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  val modeData = combined_df.groupBy(col(column)).count()
  val maxCount = modeData.agg(max("count")).first().getLong(0)
  val modes = modeData.filter(col("count") === maxCount).select(column).as[Double].collect().map(x => BigDecimal(x).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble).mkString(", ")
  val stddevValue = BigDecimal(combined_df.agg(stddev(col(column))).first().getDouble(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  statsResults += ((column, meanValue, median, modes, stddevValue))
}
val stats_df_test = statsResults.toSeq.toDF("Column", "Mean", "Median", "Mode", "Standard Deviation")
println("Training Data Numerical Summary")
stats_df_test.show(21, false)

// calculate statistics for each column, including multiple nodes if applicable
numericalColumns.foreach { column =>
  val meanValue = BigDecimal(df23.agg(mean(col(column))).first().getDouble(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  val median = BigDecimal(df23.stat.approxQuantile(column, Array(0.5), 0.0)(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  val modeData = df23.groupBy(col(column)).count()
  val maxCount = modeData.agg(max("count")).first().getLong(0)
  val modes = modeData.filter(col("count") === maxCount).select(column).as[Double].collect().map(x => BigDecimal(x).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble).mkString(", ")
  val stddevValue = BigDecimal(df23.agg(stddev(col(column))).first().getDouble(0)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  statsResults += ((column, meanValue, median, modes, stddevValue))
}
val stats_df_test = statsResults.toSeq.toDF("Column", "Mean", "Median", "Mode", "Standard Deviation")
println("Testing Data Numerical Summary")
stats_df_test.show(21, false)