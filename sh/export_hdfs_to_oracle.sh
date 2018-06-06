#!/bin/bash

function print_message(){
   verde="\e[92m"
   default="\e[39m"
   echo "${verde}${1}${default}"
}

#Auhor : Arturo Gonzalez Becerril
#Version : 0.1
#Decripcion: Script que invoca un spark-shell para ejecutar los comandos que contienen los ficheros $1 y $2

#Argumentos
# $1 - Libreria de uso general
# $2 - Codigo en scala que sera ejecutado por el shell de spark
# $3 - Path donde se encuentra el binario para lanzar un shell de spark.

print_message "Invocando el shell de spark"

cat $1 $2 | $3

print_message "Script de spark-shell terminado"
