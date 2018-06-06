#!/bin/bash

#Author: Arturo Gonzalez Becerril
#Version: 0.1
#Parametros
#
#  $1   stored_directory_hdfs
#  $2   partition_save_file_tmp
#  $3   tables_document_oracle_tmp
#  $4   files_java


# ************* Informacion de directorio final donde se almacena al informacion particionada por fecha ***************
# Ficheros temporales que se construyen en cada ejecucion del script.
#  - save_partition_file.scala --> Contiene codigo para almacenar en hdfs y particionar la info por anio y mes.
#  - tables.txt         --> Contiene el nombre de las tablas que se estan exportando de oracle a HDFS.


############################ Funciones de uso general #################################################################

function print_message(){
   verde="\e[92m"
   default="\e[39m"
   echo "${verde}${1}${default}"
}


function remove_file(){
     output_command=$(rm $1)
}

function file_exist(){
    get_date=$(date +"%Y-%m-%d %H:%M:%S,%3N")
    message=""
    if [ -e $1 ]
     then
         message=$(remove_file $1)
         print_message "$get_date Removiendo ficheros temporales '$1' $message"
     else
         print_message "$get_date Eliminando ficheros temporales ...$message"
    fi
}

function remove_all_files_temp(){
 IFS=';' read -ra file_temp <<< "$1"

 for i in "${file_temp[@]}"; do
    file_exist $i
 done
}

################################### Ficheros temporales  ###############################################################
stored_directory_hdfs=${1}
partition_save_file_tmp=${2}
tables_document_oracle_tmp=${3}
files_java=${4}

####################################  Declaración de variables  ########################################################
url_jdbc=${5}
user=${6}
password=${7}
table=${8}
columns=${9}

# ************* INFORMACION DE DESTINO TEMPORAL EN HIVE HDFS ***********************************************************
path_hdfs_hive=${10}
schema_hive=${11}
table_hive=${12} #El nombre origen sera igual al nombre destino.

#  ****************************** PARAMETROS PROPIOS DE SQOOP **********************************************************
split_by=${13}
number_mappers=${14}
data_type_columns=${15}


#********* SCRIPT EN SQOOP PARA IMPORTAR DATOS DE ORACLE A UNA TABLA HIVE **********************************************
sqoop import --connect ${url_jdbc} --username ${user} --password ${password} --table ${table} --columns ${columns} --map-column-hive ${data_type_columns} --warehouse-dir ${path_hdfs_hive} --split-by ${split_by} --hive-import --hive-table ${schema_hive}"."${table} --num-mappers ${number_mappers} --compression-codec org.apache.hadoop.io.compress.SnappyCodec


# **********************     ELIMINAR FICHEROS TEMPORALES, SI ES QUE EXISTEN *******************************************
remove_all_files_temp "${partition_save_file_tmp};${tables_document_oracle_tmp};${files_java}"
