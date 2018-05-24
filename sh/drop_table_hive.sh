#!/bin/bash

#This script require 1 argument (table name hive)

if output=$(hive -e "drop table $1"); then
    echo "Salida: $output"
else
    echo "Fallo al lanzar el comando, $output"
fi

#hive -e "drop table $1"
