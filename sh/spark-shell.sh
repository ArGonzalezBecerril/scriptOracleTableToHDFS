#!/bin/bash

bash spark-shell --master yarn --executor-memory 12G  --jars lib/spark-csv_2.10-1.5.0.jar,lib/univocity-parsers-2.1.2.jar,lib/commons-csv-1.4.jar --conf spark.ui.showConsoleProgress=true --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops -XX:+UseG1GC"
