# Video Presentation Script — 15–17 minutes

## 1. Introduction (1.5 min)
This project demonstrates a distributed banking data workflow. The business question for the historical dataset is whether a customer subscribes to a term deposit after a marketing campaign. The architecture uses Hadoop HDFS for distributed storage, Hive for SQL-style access, Apache Spark for batch analysis, Spark MLlib for predictive modeling, and Spark Structured Streaming for synthetic transaction monitoring. These components have different responsibilities; the purpose is to show how they fit together.

## 2. Problem understanding (2 min)
Banking systems create data with high volume, variety, and velocity. A single-machine workflow may become limited by memory, CPU, reliability, and the speed at which new events must be processed. Distributed computing splits work across partitions and executors. Batch analytics is useful for historical trends and model training; streaming supports rapid operational visibility. The provided file is only 4,521 rows, so it is a learning dataset, not proof of production-scale performance.

## 3. Storage and processing architecture (2 min)
[Show the architecture diagram.] The CSV is uploaded to HDFS. Hive defines an external table and exposes HiveQL while the files remain in HDFS. Spark reads the same data for EDA and ML pipelines. Separately, generated JSON transaction events are read incrementally by Structured Streaming. The pipeline separates storage, query, batch analytics, prediction, and streaming responsibilities.

## 4. Spark EDA (2.5 min)
[Run Part 2 and show actual output.] Spark checks the schema, row count, missing values, target balance, numeric summaries, and subscription rates by job and month. The supplied data has 4,521 rows, 17 columns, and no missing cells. There are 4,000 `no` labels and 521 `yes` labels, so the positive rate is about 11.5%. The special value `pdays=-1` means no prior contact; it should be treated as a meaningful category or converted to a separate indicator. Spark uses lazy transformations, and actions trigger execution across partitions.

## 5. Predictive modeling (2 min)
[Show Part 3 code and actual metrics.] The target is term-deposit subscription, not loan default. The pipeline encodes categorical columns, imputes numeric fields, assembles features, and trains Logistic Regression and Random Forest. The dataset is split into training and test sets with a fixed seed. Metrics include ROC-AUC and PR-AUC; PR-AUC and positive-class precision/recall are especially important because the target is imbalanced. The primary model excludes `duration`, because call duration is not known before the call and could leak future information. Only quote metrics produced by the actual run.

## 6. Streaming and parallelism (2 min)
[Show the JSON samples and Part 4 notebook.] Synthetic transaction records include timestamp, account, amount, channel, and country. Structured Streaming processes new files incrementally, checkpoints progress, applies a transparent illustrative alert rule, and calculates one-minute aggregates. This is a demonstration, not a real fraud detector. In distributed execution, partitions can be processed concurrently. Repartitioning can improve parallelism but also triggers a shuffle.

## 7. Challenges, optimization and trade-offs (1.5 min)
Typical challenges include Java/Spark compatibility, container readiness, correct HDFS paths, checkpoint management, and resource limits. Optimizations include filtering early, selecting necessary columns, avoiding repeated actions, and choosing partitions carefully. More partitions can improve concurrency but create scheduling and shuffle overhead. The small dataset does not justify performance claims; a controlled benchmark on a larger dataset would be needed.

## 8. Learnings and practical value (1.5 min)
The project demonstrates how distributed storage, SQL access, batch analytics, ML, and streaming fit into one architecture. It also shows the importance of avoiding leakage, evaluating minority classes properly, and distinguishing educational prototypes from production systems. Production improvements would include a Kafka or Event Hubs source, stronger data-quality checks, model tuning, explainability, monitoring, access controls, cloud deployment, and CI/CD.

## Demonstration checklist
- Show HDFS file listing and Hive row-count output.
- Show Spark schema, null profile, target distribution, and conversion-rate tables.
- Show the real `metrics.json` / model metrics from the ML notebook.
- Show sample JSON events and actual streaming alerts/window output.
- Explain limitations honestly and finish with future improvements.
