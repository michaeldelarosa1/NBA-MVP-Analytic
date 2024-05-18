val trainData = outputData_train
val testData = outputData_test

// Define Linear Regression model 
val lr = new LinearRegression().setMaxIter(100).setRegParam(0.01).setFeaturesCol("features")

//fitting the model 
val model = lr.fit(trainData)

val predictions = model.transform(testData)

// This section evaluates our model
val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

val evaluatorR2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")

// Compute R^2 for the model
val r2 = evaluatorR2.evaluate(predictions)

println(s"Coefficient of determination R^2 on test data = $r2")

// Analyzing the coefficients
val coefficients = model.coefficients.toArray

// Output the coefficients along with feature names
println()
println(s"Coefficients for Linear Regression")
featureColumns_test.zip(coefficients).foreach { case (feature, coeff) =>
  println(s"Coefficient for $feature is $coeff")
}

// Showing predicitons with players
val predictionsWithPlayer = predictions.join(post_numerical_test.drop("label").filter($"GP" >= 65), "PLAYER").select("PLAYER", "prediction", "label", "GP")

// Show predictions with player names
println()
println(s"Predictions for Linear Regression")
predictionsWithPlayer.orderBy(desc("prediction")).show(20, false)

// Define Linear Regression model 
val ridge = new LinearRegression().setMaxIter(10).setRegParam(0.01).setElasticNetParam(0.0).setFeaturesCol("features")

//fitting the model 
val model = ridge.fit(trainData)

val ridge_predictions = model.transform(testData)

// This section evaluates our model
val ridge_evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

val ridge_rmse = ridge_evaluator.evaluate(ridge_predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $ridge_rmse")

val evaluatorR2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")

// Compute R^2 for the model
val ridge_r2 = evaluatorR2.evaluate(ridge_predictions)

println(s"Coefficient of determination R^2 on test data = $ridge_r2")

// Analyzing the coefficients
val ridge_coefficients = model.coefficients.toArray

// Output the coefficients along with feature names
println()
println(s"Coefficients for Ridge Regression")
featureColumns_test.zip(ridge_coefficients).foreach { case (feature, coeff) =>
  println(s"Coefficient for $feature is $coeff")
}

// Showing predicitons with players
val predictionsWithPlayer = ridge_predictions.join(post_numerical_test.drop("label").filter($"GP" >= 65), "PLAYER").select("PLAYER", "prediction", "label", "GP")

// Show predictions with player names
println()
println(s"Predictions for Ridge Regression")
predictionsWithPlayer.orderBy(desc("prediction")).show(20, false)

// Define Linear Regression model 
val lasso = new LinearRegression().setMaxIter(10).setRegParam(0.01).setElasticNetParam(1.0).setFeaturesCol("features")

//fitting the model 
val model = lasso.fit(trainData)

val lasso_predictions = model.transform(testData)

// This section evaluates our model
val lasso_evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

val lasso_rmse = lasso_evaluator.evaluate(lasso_predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $lasso_rmse")

val evaluatorR2 = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("r2")

// Compute R^2 for the model
val lasso_r2 = evaluatorR2.evaluate(lasso_predictions)

println(s"Coefficient of determination R^2 on test data = $lasso_r2")

// Analyzing the coefficients
val lasso_coefficients = model.coefficients.toArray

// Output the coefficients along with feature names
println()
println(s"Coefficients for Lasso Regression")
featureColumns_test.zip(lasso_coefficients).foreach { case (feature, coeff) =>
  println(s"Coefficient for $feature is $coeff")
}

// Showing predicitons with players
val predictionsWithPlayer = lasso_predictions.join(post_numerical_test.drop("label").filter($"GP" >= 65), "PLAYER").select("PLAYER", "prediction", "label", "GP")

// Show predictions with player names
println()
println(s"Predictions for Lasso Regression")
predictionsWithPlayer.orderBy(desc("prediction")).show(20, false)
