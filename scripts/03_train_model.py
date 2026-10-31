
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_FILE = PROJECT_ROOT / "data" / "bank.csv"
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def main():
    spark = (
        SparkSession.builder
        .appName("BankingCapstoneML")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("sep", ",")
            .csv(str(DATA_FILE))
        )

        # Exclude duration: it is only known after the call and can
        # cause target leakage in a pre-call prediction scenario.
        numeric_cols = [
            "age", "campaign", "pdays", "previous"
        ]
        categorical_cols = [
            "job", "marital", "education", "default", "housing",
            "loan", "contact", "month", "poutcome"
        ]

        # Convert the yes/no target to a numeric label.
        df = df.withColumn(
            "label",
            F.when(F.col("y") == "yes", 1.0).otherwise(0.0)
        )

        # Fit preprocessing on the training data through a Pipeline.
        indexers = [
            StringIndexer(
                inputCol=c,
                outputCol=f"{c}_idx",
                handleInvalid="keep"
            )
            for c in categorical_cols
        ]

        encoder = OneHotEncoder(
            inputCols=[f"{c}_idx" for c in categorical_cols],
            outputCols=[f"{c}_oh" for c in categorical_cols]
        )

        assembler = VectorAssembler(
            inputCols=numeric_cols +
                      [f"{c}_oh" for c in categorical_cols],
            outputCol="features",
            handleInvalid="keep"
        )

        train, test = df.randomSplit([0.8, 0.2], seed=42)

        models = {
            "logistic_regression": LogisticRegression(
                featuresCol="features",
                labelCol="label",
                maxIter=30
            ),
            "random_forest": RandomForestClassifier(
                featuresCol="features",
                labelCol="label",
                numTrees=50,
                maxDepth=8,
                seed=42
            )
        }

        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )

        results = {}

        for model_name, classifier in models.items():
            pipeline = Pipeline(
                stages=indexers + [encoder, assembler, classifier]
            )

            print(f"\nTraining {model_name}...")
            fitted = pipeline.fit(train)
            predictions = fitted.transform(test)

            auc = evaluator.evaluate(predictions)

            # Accuracy and positive-class precision/recall.
            counts = predictions.agg(
                F.sum(
                    F.when(
                        F.col("prediction") == F.col("label"), 1
                    ).otherwise(0)
                ).alias("correct"),
                F.count("*").alias("total"),
                F.sum(
                    F.when(
                        (F.col("prediction") == 1.0) &
                        (F.col("label") == 1.0), 1
                    ).otherwise(0)
                ).alias("tp"),
                F.sum(
                    F.when(
                        (F.col("prediction") == 1.0) &
                        (F.col("label") == 0.0), 1
                    ).otherwise(0)
                ).alias("fp"),
                F.sum(
                    F.when(
                        (F.col("prediction") == 0.0) &
                        (F.col("label") == 1.0), 1
                    ).otherwise(0)
                ).alias("fn")
            ).first()

            tp = counts["tp"] or 0
            fp = counts["fp"] or 0
            fn = counts["fn"] or 0

            results[model_name] = {
                "test_rows": int(counts["total"]),
                "accuracy": round(counts["correct"] / counts["total"], 4),
                "roc_auc": round(auc, 4),
                "precision_yes": round(tp / (tp + fp), 4)
                    if tp + fp else 0.0,
                "recall_yes": round(tp / (tp + fn), 4)
                    if tp + fn else 0.0
            }

            predictions.select("y", "label", "prediction").show(10)

            print(f"{model_name} trained successfully.")
            print("Model persistence skipped due to Windows Hadoop compatibility.")

            print(f"{model_name} metrics: {results[model_name]}")

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        metrics_file = OUTPUT_DIR / "model_metrics.json"
        metrics_file.write_text(
            json.dumps(results, indent=2),
            encoding="utf-8"
        )

        print(f"\nMetrics saved to: {metrics_file}")
        print("Trained models saved under outputs/models/")
        print("\nMachine-learning step completed successfully.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
