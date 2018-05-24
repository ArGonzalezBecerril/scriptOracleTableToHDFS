#!/bin/bash

# Version 0.1
# Author:Arturo Gonzalez Becerril

# *****Descripcion*******************************************************************************
# This Script  build pool connections to oracle
# Objetive: Is nessesary for export data hive to Oracle.

 if [ $# -ne 3 ]
   then
     echo "No se puede construir el fichero init.scala, el numero de parametros es incorrecto"
     exit 1
   else
     table_name_oracle=$1
     schema_hive=$2
     path_hdfs_data_partition=$3
     touch ../init.scala && touch tables_oracle.txt
     init_path="../init.scala"
     tables_oracle="tables_oracle.txt"

     echo "println(\" Exportando y particionando la tabla $table_name_oracle a hdfs\")" >> $init_path
     echo "val cliente = hiveContext.sql(\"select * from $schema_hive"."$table_name_oracle\")" >> $init_path
     echo "val dirHDFSCliente:String=\"$path_hdfs_data_partition\"" >> $init_path
     echo "cliente.toDF().selectExpr(\"*\",\"substring(id_mes,0,4) as year\", \"substring(id_mes,5,2) as month\").repartition(10).write.mode(SaveMode.Append).partitionBy(\"year\",\"month\").mode(SaveMode.Append).parquet(\"$path_hdfs_data_partition\")" >> $init_path
     #Add tables oracle will by remove at the end Script
     echo "$table_name_oracle"  >> $tables_oracle
 fi
