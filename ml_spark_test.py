from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
import sys
#from xgboost.spark import SparkXGBClassifier
from datetime import datetime

today_date = datetime.now().strftime('%Y-%m-%d')

db_user = "x"
db_password = "x"

spark = SparkSession.builder.appName("machine_learning_by_spark").getOrCreate()

jdbc_url = "jdbc:postgresql://x.x.x.x:x/x"
jdbc_properties = {
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=jdbc_url, table="loan_credit", properties=jdbc_properties)

def preprocess_ordinal_encoder(df): # Buat proses data2 ordinal category
    # Ordinal encoding
    df = df.withColumn(
        "employment_status_ord",
        when(col("employment_status") == "unemployed", 0)
        .when(col("employment_status") == "contract", 1)
        .when(col("employment_status") == "permanent", 2)
    )

    df = df.withColumn(
        "education_level_ord",
        when(col("education_level") == "high_school", 0)
        .when(col("education_level") == "bachelor", 1)
        .when(col("education_level") == "master", 2)
    )

    return df

df = preprocess_ordinal_encoder(df)

indexer_property_area = StringIndexer(inputCol="property_area", outputCol="property_area_idx", handleInvalid="keep")
indexer_marital_status = StringIndexer(inputCol="marital_status", outputCol="marital_status_idx", handleInvalid="keep")

feature_cols = [
	"age",
	"income",
	"loan_amount",
	"loan_term",
	"credit_score",
	"number_of_dependents",
	"has_prior_loans",
	"prior_loan_defaults",
	"employment_status_ord",
	"education_level_ord",
	"property_area_idx",
	"marital_status_idx"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

models = {
    "Logistic Regression": LogisticRegression(featuresCol="features", labelCol="approval_status", maxIter=10),
    "Decision Tree": DecisionTreeClassifier(featuresCol="features", labelCol="approval_status", maxDepth=5),
    "Random Forest": RandomForestClassifier(featuresCol="features", labelCol="approval_status", numTrees=100),
    "Gradient Boosted Trees": GBTClassifier(featuresCol="features", labelCol="approval_status", maxIter=50),
    #"Xtreme Gradient Boost": SparkXGBClassifier(features_col="features", label_col="approval_status", n_estimators=100, max_depth=5)
}


results = []

for model_name, classifier in models.items():
    pipeline = Pipeline(stages=[
        indexer_property_area,
        indexer_marital_status,
        assembler,
        classifier
    ])
    
    model = pipeline.fit(train_data)
    
    test_pred = model.transform(test_data)
    
    roc_evaluator = BinaryClassificationEvaluator(
        labelCol="approval_status",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    
    acc_evaluator = MulticlassClassificationEvaluator(
        labelCol="approval_status",
        predictionCol="prediction",
        metricName="accuracy"
    )
    
    test_accuracy = acc_evaluator.evaluate(test_pred)
    test_roc_auc = roc_evaluator.evaluate(test_pred)
    
    results.append((model_name, test_accuracy, test_roc_auc))

#################################################################################################
# All models results
#################################################################################################

print(f"{'Model':30} | {'Test Acc':10} | {'Test ROC AUC':15}")
print("-" * 75)
for model_name, test_acc, test_roc_auc in results:
    print(f"{model_name:30} | {test_acc:<10.4f} | {test_roc_auc:<15.4f}")

# Get best model by ROC AUC metric
best_model_name, *_ , best_roc_auc = max(results, key=lambda x: x[2])

print("\nBest Model:")
print(f"Model: {best_model_name} with ROC AUC: {best_roc_auc:.4f}")


#################################################################################################
# Start Predict New Data
#################################################################################################

model_best = models[best_model_name]
pipeline_best = Pipeline(stages=[
    indexer_property_area,
    indexer_marital_status,
    assembler,
    model_best
])

df_new_loan = spark.read.jdbc(url=jdbc_url, table="loan_credit_2", properties=jdbc_properties)
df_new_loan = preprocess_ordinal_encoder(df_new_loan)

model_best_trained = pipeline_best.fit(df)
predictions = model_best_trained.transform(df_new_loan)

#################################################################################################
# CSV Result
#################################################################################################

predictions_for_csv = predictions.withColumnRenamed("prediction", "predicted_approval_status").withColumnRenamed("insert_date", "application_received")

predictions_for_csv = predictions_for_csv.select("applicant_id", "predicted_approval_status", "application_received")


predictions_for_csv.write.mode("overwrite").orc(f"hdfs:///loan_credit/data_applicant_today/data_applicant_{today_date}.csv")

print("""
#################################################################################################
################################### Dump CSV to HDFS ############################################
#################################################################################################
""")

#################################################################################################
# Write to Postgre's Table
#################################################################################################

pred_result = predictions.select("applicant_id", "prediction")
df_new_loan = spark.read.jdbc(url=jdbc_url, table="loan_credit_2", properties=jdbc_properties)
df_join = df_new_loan.join(pred_result, on="applicant_id", how="left")

predictions_postgres = df_join.withColumn(
    "approval_status",
    when(col("approval_status").isNull(), col("prediction")).otherwise(col("approval_status"))
).drop("prediction")

predictions_postgres.write.jdbc(url=jdbc_url, table="loan_credit", mode="append", properties=jdbc_properties)

print("""
#################################################################################################
################## Sending New Data to Table \"Credit Loan\" is Complete ##########################
#################################################################################################
""")

#################################################################################################
# Dump to HDFS
#################################################################################################

predictions_postgres.write.mode("overwrite").orc("hdfs:///loan_credit/data_applicant/data_applicant.orc")

print("""
#################################################################################################
################################### Dump to HDFS is Complete ####################################
#################################################################################################
""")


#################################################################################################
# Delete Postgre's Table Data
#################################################################################################

empty_df = spark.createDataFrame([], df_new_loan.schema)

empty_df.write.jdbc(
    url=jdbc_url,
    table="loan_credit_2",
    mode="overwrite",
    properties=jdbc_properties
)

print("""
#################################################################################################
#################### Deleting New Data in Original Table is Complete ############################
#################################################################################################
""")

spark.stop()
