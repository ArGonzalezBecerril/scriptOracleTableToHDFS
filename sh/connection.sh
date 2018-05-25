#!/bin/bash

# Version 0.1
# Author:Arturo Gonzalez Becerril

# *****      DESCRIPTION     *******************************************************************************************
# This Script  build pool connections to oracle
# Params
#  $1 Table name oracle exported
#  $2 Schema that will contain table hive
#  $3 Path hdfs will save tables
#  $4 File that contains scala commands will run into spark-shell
#  $5
#
# Objective: This file is necessary for export data from hive to HDFS.

 if [ $# -ne 5 ]
   then
     echo "No se puede construir el fichero init.scala, el numero de parametros es incorrecto"
     exit 1
   else
     table_name_oracle=$1
     schema_hive=$2
     path_hdfs_data_partition=$3
     commands_scala=$4
     #Create file that will contains commands scala
     touch $4 && touch $5

     commands_scala=$4
     tables_oracle=$5

     echo "println(\" Exportando y particionando la tabla $table_name_oracle a hdfs\")" >> $commands_scala
     echo "val cliente = hiveContext.sql(\"select * from $schema_hive"."$table_name_oracle\")" >> $commands_scala
     echo "val dirHDFSCliente:String=\"$path_hdfs_data_partition\"" >> $commands_scala
     echo "cliente.toDF().selectExpr(\"*\",\"substring(id_mes,0,4) as year\", \"substring(id_mes,5,2) as month\").repartition(10).write.mode(SaveMode.Append).partitionBy(\"year\",\"month\").mode(SaveMode.Append).parquet(\"$path_hdfs_data_partition\")" >> $commands_scala
     #Add tables oracle will by remove at the end Script
     echo "$schema_hive"."$table_name_oracle"  >> $tables_oracle
 fi
