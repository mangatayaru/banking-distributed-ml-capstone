# Banking Distributed Computing Capstone Submission

## Package contents
- Five separate Colab/Jupyter notebooks, one per assessment part
- Hive DDL and analysis queries
- Spark Structured Streaming event generator and streaming consumer
- 15–17 minute presentation script
- Reflection document
- Source dataset copied to `data/bank.csv`

## Verified dataset facts from the uploaded CSV
- Rows: 4521
- Columns: 17
- Missing cells: 0
- Target counts: {'no': 4000, 'yes': 521}
- Positive class rate: 11.52%

## Run in Google Colab
Upload the ZIP to Colab and extract it, or upload the individual notebooks and dataset. For Spark notebooks, install `pyspark==3.5.3` and ensure Java is available. The Hadoop/Hive notebook contains Docker commands for local execution; Docker itself generally runs on your own computer, not a standard Colab runtime.

## Run locally
1. Install Java compatible with your chosen Spark version and Python 3.10+.
2. Install PySpark: `pip install pyspark==3.5.3`.
3. Run the notebooks in order.
4. Run the Hive SQL in Beeline after the HDFS upload.
5. Generate synthetic stream data and launch `scripts/05_stream_transactions.py`.

## Honesty / reproducibility
The uploaded data profile is verified. Hadoop/Hive and Spark execution outputs must be captured from your own run; this package does not fabricate those outputs or claim a cluster was run here. Record real Spark ML metrics in the video and reflection. This dataset supports term-deposit marketing prediction, not loan-default prediction. Streaming alerts use synthetic transactions and illustrative rules, not a validated fraud detector.
