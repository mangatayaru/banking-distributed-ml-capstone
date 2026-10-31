# Reflective Summary — Banking Distributed Computing Capstone

## Summary
This project links HDFS storage, Hive querying, Spark EDA, Spark MLlib, Structured Streaming, and data-parallel processing in a banking marketing scenario.

## Verified source data facts
- Rows: 4521
- Columns: 17
- Missing cells: 0
- Target counts: {'no': 4000, 'yes': 521}
- Positive target rate: 11.52%

## Challenges and learning outcomes
The main challenges expected in a local setup are aligning Java, Spark, and container versions; confirming HDFS locations; understanding Spark's lazy execution; handling streaming checkpoints; and choosing partitions that fit the workload. The target is imbalanced, so accuracy alone would be a weak metric. The `pdays=-1` sentinel has a specific meaning and should be handled intentionally. `duration` is excluded from pre-contact prediction to reduce target leakage.

The project reinforces that HDFS is storage, Hive provides a query/metadata layer, Spark supports distributed batch computation, Spark MLlib packages preprocessing and model training, and Structured Streaming processes incrementally arriving events. Parallelism can improve throughput, but shuffles and tiny partitions can reduce efficiency.

## Complete after running
Add actual errors and fixes, screenshots of HDFS/Hive/Spark execution, model evaluation metrics, and observed runtime/partition results. Do not present planned or expected outcomes as executed results.

## Future improvements
Use a larger benchmark dataset; add cross-validation and per-class metrics; explain model predictions; use Kafka or Azure Event Hubs; add robust schema validation and durable alert sinks; and deploy with monitoring, access control, privacy review, and CI/CD.
