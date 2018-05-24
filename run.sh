#!/bin/bash

#If any command fail the script shell exit to immediately
set -e

########################################################################################################################

#Call script in python
echo "Exportando las tablas de oracle hive"
python import_oracle_to_hive.py

#######################################################################################################################
#Call scripts scala and get spark-shell
echo "Particionando las tablas en hdfs"
cat scala/header.scala init.scala | ./sh/spark-shell.sh

echo "Eliminado las tablas temporales en hive"
while IFS= read table_name
do
    bash sh/drop_table_hive.sh $table_name
done < sh/tables_oracle.txt

#######################################################################################################################
#Remove all files temporary
rm init.scala && rm sh/tables_oracle.txt
echo "Script finalizado"




