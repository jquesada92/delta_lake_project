#!/bin/sh
pyspark --packages io.delta:delta-spark_2.12:3.1.0 --driver-memory 5g --executor-memory 5g --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"  --conf "spark.executor.pyspark.memory=1g" 
