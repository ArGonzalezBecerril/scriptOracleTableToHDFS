#!/bin/bash

# Author: Arturo Gonzalez Becerril
# Version: 0.1
# Script principal el cual llama a todos los subprocesos en bash


#**************************** FUNCIONES DE USO GENERAL ****************************************************************#

function are_there_tables(){
    local list_tables=""
    if [ $# -ne 10 ]
        then
            print_message "Argumentos incompletos, es necesario el nombre de las tablas"
            exit 1
        else
            list_tables=$1
    fi
    return ${list_tables}
}

function print_message(){
   echo "$1"
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
         print_message "$get_date ...$message"
    fi
}

function remove_all_files_temp(){
 IFS=';' read -ra file_temp <<< "$1"

 for i in "${file_temp[@]}"; do
    file_exist ${i}
 done
}


function get_tables(){
 IFS=';' read -ra file_temp <<< "$1"

 for i in "${file_temp[@]}"; do
    file_exist ${i}
 done
}
#####################################INICIO ############################################################################

#***********************************  Fichero temporales ***************************************************************

stored_directory_hdfs="/pro/workspace/crm/gestion_clientes"      # $1
processing_to_hdfs="tmp/processing_to_hdfs.scala"                # $2
exported_tables="tmp/exported_tables.txt"                        # $3
files_java="*.java"                                              # $4

# ****************** CONEXION A ORACLE (ORIGEN DE LOS DATOS) ***********************************************************
url_jdbc="jdbc:oracle:thin:@180.181.182.41:1640:mcrminer"        # $5
user="USRLINK"                                                   # $6
password="linkusr01"                                             # $7
table=are_there_tables ${1}                                      # $8
columns="ID_MES,ID_CLIENTE,ID_SEGMENTO,ID_EJECUTIVO,ID_SUCURSAL,CHEQUES,CHEQUES_CTAS,CHEQUES_TDD,CHEQUES_CTAS_CERO,CHEQUES_TDD_CERO,CHEQUES_PROM,CHEQUES_PUNT,NOMINA,NOMINA_CTAS,NOMINA_TDD,NOMINA_CTAS_CERO,NOMINA_TDD_CERO,NOMINA_PROM,NOMINA_PUNT,UNIVERSIDADES,UNIVERSIDADES_CTAS,UNIVERSIDADES_TDD,UNIV_CTAS_CERO,UNIV_TDD_CERO,UNIVERSIDADES_PROM,UNIVERSIDADES_PUNT,AHORRO,AHORRO_CTAS,AHORRO_CTAS_CERO,AHORRO_PROM,AHORRO_PUNT,CASH,CASH_CTAS,CASH_CTAS_CERO,CASH_PROM,CASH_PUNT,JUNIOR,JUNIOR_CTAS,JUNIOR_TDD,JUNIOR_CTAS_CERO,JUNIOR_TDD_CERO,JUNIOR_PROM,JUNIOR_PUNT,INVERSION_VISTA,INVERSION_VISTA_CTAS,INV_VISTA_CTAS_CERO,INVERSION_VISTA_PROM,INVERSION_VISTA_PUNT,PLAZO,PLAZO_CTAS,PLAZO_PROM,PLAZO_PUNT,FONDOS,FONDOS_CTAS,FONDOS_CTAS_CERO,FONDOS_PROM,FONDOS_PUNT,PLAZO_OP,PLAZO_OP_CTAS,PLAZO_OP_PROM,PLAZO_OP_PUNT,FONDOS_OP,FONDOS_OP_CTAS,FONDOS_OP_CTAS_CERO,FONDOS_OP_PROM,FONDOS_OP_PUNT,MDD,MDD_CTAS,MDD_PROM,MDD_PUNT,HIPOTECARIO,HIPO_CTAS,HIPO_PROM,HIPO_PUNT,CRED_EFECTIVO,CRED_EFEC_CTAS,CRED_EFEC_PROM,CRED_EFEC_PUNT,CRED_NOMINA,CRED_NOM_CTAS,CRED_NOM_PROM,CRED_NOM_PUNT,LCI_EFEC,LCI_EFEC_CTAS,LCI_EFEC_PROM,LCI_EFEC_PUNT,LCI_NOM,LCI_NOM_CTAS,LCI_NOM_PROM,LCI_NOM_PUNT,AUTOS,AUTOS_CTAS,AUTOS_PROM,AUTOS_PUNT,PLDC,PLDC_CTAS,PLDC_PROM,PLDC_PUNT,CRED_SIMPLE,CRED_SIM_CTAS,CRED_SIM_PROM,CRED_SIM_PUNT,CRED_COMERCIAL,CRED_COM_CTAS,CRED_COM_PROM,CRED_COM_PUNT,AGIL,AGIL_CTAS,AGIL_PROM,AGIL_PUNT,OTROCOL,OTROCOL_CTAS,OTROCOL_PROM,OTROCOL_PUNT,COMEX,COMEX_CTAS,COMEX_PROM,COMEX_PUNT,TDC,TDC_CTAS,TDC_PROM,TDC_PUNT,LEX,LEX_CTAS,LEX_PROM,LEX_PUNT,BALANCE_TRASNFER,NUM_TRANSACCIONES,NUM_TRANS_VTX,COMPRAS,PAGOS,TRANSFERENCIAS,CORE_DEPOSIT,TRANSFERENCIA_ATM,MARGEN_VISTA,MARGEN_PLAZO,MARGEN_CARTERA,MARGEN_CONSUMO,MARGEN_TDC,MARGEN_HIPOTECARIO,COM_SEGUROS,COM_CASH_MANAGEMENT,COM_ING_FONDOS,COM_COMERCIO_EXTERIOR,COM_TRASLADO_PERDIDA,COM_RESTO,ADM_GESTION_CTAS,TDC_INTERCAMBIO_PLC,TDC_CUOTA_MENSUAL,TDC_COM_SOBRETASA_MSI,RESTO_MEDIOS_PAGO,MBB,PLDC_3M,ID_ACTIVO,ID_CTES_CEROS,ID_DOMI,ID_CR_TDD_BASICAS,ID_CR_TDC_BASICAS,ID_CR_TDD_COMPLEMENTARIAS,ID_CR_TDC_COMPLEMENTARIAS,ID_CR_TDD_RESTO,ID_CR_TDC_RESTO,ID_DOM_TDD_BASICAS,ID_DOM_TDC_BASICAS,ID_DOM_TDD_COMPLEMENTARIAS,ID_DOM_TDC_COMPLEMENTARIAS,ID_DOM_TDD_RESTO,ID_DOM_TDC_RESTO,ID_DOM_TDD_PAGOTDC,ID_CR_TDD_BASICAS_3M,ID_CR_TDC_BASICAS_3M,ID_CR_TDD_COMPLEMENTARIAS_3M,ID_CR_TDC_COMPLEMENTARIAS_3M,ID_CR_TDD_RESTO_3M,ID_CR_TDC_RESTO_3M,NUM_CR_TDD_BASICAS,NUM_CR_TDC_BASICAS,NUM_CR_TDD_COMPLEMENTARIAS,NUM_CR_TDC_COMPLEMENTARIAS,NUM_CR_TDD_RESTO,NUM_CR_TDC_RESTO,NUM_DOM_TDD_BASICAS,NUM_DOM_TDC_BASICAS,NUM_DOM_TDD_COMPLEMENTARIAS,NUM_DOM_TDC_COMPLEMENTARIAS,NUM_DOM_TDD_RESTO,NUM_DOM_TDC_RESTO,NUM_DOM_TDD_PAGOTDC,NUM_CR_TDD_BASICAS_3M,NUM_CR_TDC_BASICAS_3M,NUM_CR_TDD_COMPLEMENTARIAS_3M,NUM_CR_TDC_COMPLEMENTARIAS_3M,NUM_CR_TDD_RESTO_3M,NUM_CR_TDC_RESTO_3M,SEG_ATM,SEG_ATM_POLIZAS,SEG_ATM_PRIMAS,SEG_SAFE,SEG_SAFE_POLIZAS,SEG_SAFE_PRIMAS,SEG_AUTO,SEG_AUTO_POLIZAS,SEG_AUTO_PRIMAS,SEG_SALUD,SEG_SALUD_POLIZAS,SEG_SALUD_PRIMAS,SEG_HOGAR,SEG_HOGAR_POLIZAS,SEG_HOGAR_PRIMAS,SEG_IPS,SEG_IPS_POLIZAS,SEG_IPS_PRIMAS,SEG_VR,SEG_VR_POLIZAS,SEG_VR_PRIMAS,SEG_FRAUDE_ANUAL,SEG_FRAUDE_ANUAL_POLIZAS,SEG_FRAUDE_ANUAL_PRIMAS,SEG_RESTO,SEG_RESTO_POLIZAS,SEG_RESTO_PRIMAS,SEG_IVS,SEG_IVS_POLIZAS,SEG_IVS_PRIMAS,SEG_ASISTENCIA,SEG_ASISTENCIA_POLIZAS,SEG_FLOTILLA,SEG_FLOTILLA_POLIZAS,SEG_FLOTILLA_PRIMAS,SEG_CRED_REL,SEG_CRED_REL_POLIZAS,SEG_COLECTIVOS,SEG_COLECTIVOS_POLIZAS,SEG_COLECTIVOS_PRIMAS,USO_SN,USO_SM,AFIL_SN,AFIL_SM,NOMINA_DISP,NOMINA_DISP_M1,NOMINA_DISP_M2,NOMINA_DISP_M3,NOMINA_DISP_ACUM,ID_CUENTA_EJE,FAM_INV_AHORRO,FAM_PLZ_FFII,FAM_MDD_IVS,FAM_TDC,FAM_HIPOTECARIO,FAM_CONSUMO_EFEC,FAM_CREDITOS,FAM_SEG_CRED,FAM_SEG_AUTO,FAM_SEG_VIDA_SALUD,FAM_SEG_RESTO,FAM_SN,FAM_SM,FAM_NOMINA,FAM_DOM_BASICAS,FAM_DOM_COMPL,FAM_TOTALES,ID_VINCULADO,ID_LOYAL,ID_TIPO_CTE_UNIV,ID_GRUPO_UNIV,ID_SUBGRUPO_UNIV,ID_PORTABILIDAD,ID_PORTABILIDAD_EFECTIVA,FLG_SANTANDER_PLUS,PORTA_DISP_M1,PORTA_DISP_M2,PORTA_DISP_M3,BANCO,FLG_SANTANDER_PLUS_CALIDAD,DES_RENTA,DES_SEGMENTO_ESP,DESC_SEGMENTO_AGRUPADO,SDO_CAPTACION,SDO_COLOCACION"

# ******************  INFORMACION DE TABLA DESTINO EN HIVE   ***********************************************************
path_hdfs_hive="/pro/workspace/crm/cliente"                      # $10
schema_hive="sb_crm"                                             # $11
table_hive=${table} #El nombre origen sera igual al nombre destin# $12

#  ****************************** PARAMETROS DE SQOOP ******************************************************************
split_by="ID_CLIENTE"                                            # $13
number_mappers="12"                                              # $14
data_type_columns="ID_MES=int,ID_CLIENTE=bigint,ID_SEGMENTO=int,ID_EJECUTIVO=string,ID_SUCURSAL=int,CHEQUES=int,CHEQUES_CTAS=int,CHEQUES_TDD=int,CHEQUES_CTAS_CERO=int,CHEQUES_TDD_CERO=int,CHEQUES_PROM=double,CHEQUES_PUNT=double,NOMINA=int,NOMINA_CTAS=int,NOMINA_TDD=int,NOMINA_CTAS_CERO=int,NOMINA_TDD_CERO=int,NOMINA_PROM=double,NOMINA_PUNT=double,UNIVERSIDADES=int,UNIVERSIDADES_CTAS=int,UNIVERSIDADES_TDD=int,UNIV_CTAS_CERO=int,UNIV_TDD_CERO=int,UNIVERSIDADES_PROM=double,UNIVERSIDADES_PUNT=double,AHORRO=int,AHORRO_CTAS=int,AHORRO_CTAS_CERO=int,AHORRO_PROM=double,AHORRO_PUNT=double,CASH=int,CASH_CTAS=int,CASH_CTAS_CERO=int,CASH_PROM=double,CASH_PUNT=double,JUNIOR=int,JUNIOR_CTAS=int,JUNIOR_TDD=int,JUNIOR_CTAS_CERO=int,JUNIOR_TDD_CERO=int,JUNIOR_PROM=double,JUNIOR_PUNT=double,INVERSION_VISTA=int,INVERSION_VISTA_CTAS=int,INV_VISTA_CTAS_CERO=int,INVERSION_VISTA_PROM=double,INVERSION_VISTA_PUNT=double,PLAZO=int,PLAZO_CTAS=int,PLAZO_PROM=double,PLAZO_PUNT=double,FONDOS=int,FONDOS_CTAS=int,FONDOS_CTAS_CERO=int,FONDOS_PROM=double,FONDOS_PUNT=double,PLAZO_OP=int,PLAZO_OP_CTAS=int,PLAZO_OP_PROM=double,PLAZO_OP_PUNT=double,FONDOS_OP=int,FONDOS_OP_CTAS=int,FONDOS_OP_CTAS_CERO=int,FONDOS_OP_PROM=double,FONDOS_OP_PUNT=double,MDD=int,MDD_CTAS=int,MDD_PROM=double,MDD_PUNT=double,HIPOTECARIO=int,HIPO_CTAS=int,HIPO_PROM=double,HIPO_PUNT=double,CRED_EFECTIVO=int,CRED_EFEC_CTAS=int,CRED_EFEC_PROM=double,CRED_EFEC_PUNT=double,CRED_NOMINA=int,CRED_NOM_CTAS=int,CRED_NOM_PROM=double,CRED_NOM_PUNT=double,LCI_EFEC=int,LCI_EFEC_CTAS=int,LCI_EFEC_PROM=double,LCI_EFEC_PUNT=double,LCI_NOM=int,LCI_NOM_CTAS=int,LCI_NOM_PROM=double,LCI_NOM_PUNT=double,AUTOS=int,AUTOS_CTAS=int,AUTOS_PROM=double,AUTOS_PUNT=double,PLDC=int,PLDC_CTAS=int,PLDC_PROM=double,PLDC_PUNT=double,CRED_SIMPLE=int,CRED_SIM_CTAS=int,CRED_SIM_PROM=double,CRED_SIM_PUNT=double,CRED_COMERCIAL=int,CRED_COM_CTAS=int,CRED_COM_PROM=double,CRED_COM_PUNT=double,AGIL=int,AGIL_CTAS=int,AGIL_PROM=double,AGIL_PUNT=double,OTROCOL=int,OTROCOL_CTAS=int,OTROCOL_PROM=double,OTROCOL_PUNT=double,COMEX=int,COMEX_CTAS=int,COMEX_PROM=double,COMEX_PUNT=double,TDC=int,TDC_CTAS=int,TDC_PROM=double,TDC_PUNT=double,LEX=int,LEX_CTAS=int,LEX_PROM=double,LEX_PUNT=double,BALANCE_TRASNFER=int,NUM_TRANSACCIONES=int,NUM_TRANS_VTX=int,COMPRAS=int,PAGOS=int,TRANSFERENCIAS=int,CORE_DEPOSIT=double,TRANSFERENCIA_ATM=double,MARGEN_VISTA=double,MARGEN_PLAZO=double,MARGEN_CARTERA=double,MARGEN_CONSUMO=double,MARGEN_TDC=double,MARGEN_HIPOTECARIO=double,COM_SEGUROS=double,COM_CASH_MANAGEMENT=double,COM_ING_FONDOS=double,COM_COMERCIO_EXTERIOR=double,COM_TRASLADO_PERDIDA=double,COM_RESTO=double,ADM_GESTION_CTAS=double,TDC_INTERCAMBIO_PLC=double,TDC_CUOTA_MENSUAL=double,TDC_COM_SOBRETASA_MSI=double,RESTO_MEDIOS_PAGO=double,MBB=double,PLDC_3M=int,ID_ACTIVO=int,ID_CTES_CEROS=int,ID_DOMI=int,ID_CR_TDD_BASICAS=int,ID_CR_TDC_BASICAS=int,ID_CR_TDD_COMPLEMENTARIAS=int,ID_CR_TDC_COMPLEMENTARIAS=int,ID_CR_TDD_RESTO=int,ID_CR_TDC_RESTO=int,ID_DOM_TDD_BASICAS=int,ID_DOM_TDC_BASICAS=int,ID_DOM_TDD_COMPLEMENTARIAS=int,ID_DOM_TDC_COMPLEMENTARIAS=int,ID_DOM_TDD_RESTO=int,ID_DOM_TDC_RESTO=int,ID_DOM_TDD_PAGOTDC=int,ID_CR_TDD_BASICAS_3M=int,ID_CR_TDC_BASICAS_3M=int,ID_CR_TDD_COMPLEMENTARIAS_3M=int,ID_CR_TDC_COMPLEMENTARIAS_3M=int,ID_CR_TDD_RESTO_3M=int,ID_CR_TDC_RESTO_3M=int,NUM_CR_TDD_BASICAS=int,NUM_CR_TDC_BASICAS=int,NUM_CR_TDD_COMPLEMENTARIAS=int,NUM_CR_TDC_COMPLEMENTARIAS=int,NUM_CR_TDD_RESTO=int,NUM_CR_TDC_RESTO=int,NUM_DOM_TDD_BASICAS=int,NUM_DOM_TDC_BASICAS=int,NUM_DOM_TDD_COMPLEMENTARIAS=int,NUM_DOM_TDC_COMPLEMENTARIAS=int,NUM_DOM_TDD_RESTO=int,NUM_DOM_TDC_RESTO=int,NUM_DOM_TDD_PAGOTDC=int,NUM_CR_TDD_BASICAS_3M=int,NUM_CR_TDC_BASICAS_3M=int,NUM_CR_TDD_COMPLEMENTARIAS_3M=int,NUM_CR_TDC_COMPLEMENTARIAS_3M=int,NUM_CR_TDD_RESTO_3M=int,NUM_CR_TDC_RESTO_3M=int,SEG_ATM=int,SEG_ATM_POLIZAS=int,SEG_ATM_PRIMAS=double,SEG_SAFE=int,SEG_SAFE_POLIZAS=int,SEG_SAFE_PRIMAS=double,SEG_AUTO=int,SEG_AUTO_POLIZAS=int,SEG_AUTO_PRIMAS=double,SEG_SALUD=int,SEG_SALUD_POLIZAS=int,SEG_SALUD_PRIMAS=double,SEG_HOGAR=int,SEG_HOGAR_POLIZAS=int,SEG_HOGAR_PRIMAS=double,SEG_IPS=int,SEG_IPS_POLIZAS=int,SEG_IPS_PRIMAS=double,SEG_VR=int,SEG_VR_POLIZAS=int,SEG_VR_PRIMAS=double,SEG_FRAUDE_ANUAL=int,SEG_FRAUDE_ANUAL_POLIZAS=int,SEG_FRAUDE_ANUAL_PRIMAS=double,SEG_RESTO=int,SEG_RESTO_POLIZAS=int,SEG_RESTO_PRIMAS=double,SEG_IVS=int,SEG_IVS_POLIZAS=int,SEG_IVS_PRIMAS=double,SEG_ASISTENCIA=int,SEG_ASISTENCIA_POLIZAS=int,SEG_FLOTILLA=int,SEG_FLOTILLA_POLIZAS=int,SEG_FLOTILLA_PRIMAS=double,SEG_CRED_REL=int,SEG_CRED_REL_POLIZAS=int,SEG_COLECTIVOS=int,SEG_COLECTIVOS_POLIZAS=int,SEG_COLECTIVOS_PRIMAS=double,USO_SN=int,USO_SM=int,AFIL_SN=int,AFIL_SM=int,NOMINA_DISP=int,NOMINA_DISP_M1=double,NOMINA_DISP_M2=double,NOMINA_DISP_M3=double,NOMINA_DISP_ACUM=double,ID_CUENTA_EJE=int,FAM_INV_AHORRO=int,FAM_PLZ_FFII=int,FAM_MDD_IVS=int,FAM_TDC=int,FAM_HIPOTECARIO=int,FAM_CONSUMO_EFEC=int,FAM_CREDITOS=int,FAM_SEG_CRED=int,FAM_SEG_AUTO=int,FAM_SEG_VIDA_SALUD=int,FAM_SEG_RESTO=int,FAM_SN=int,FAM_SM=int,FAM_NOMINA=int,FAM_DOM_BASICAS=int,FAM_DOM_COMPL=int,FAM_TOTALES=int,ID_VINCULADO=int,ID_LOYAL=int,ID_TIPO_CTE_UNIV=int,ID_GRUPO_UNIV=string,ID_SUBGRUPO_UNIV=string,ID_PORTABILIDAD=int,ID_PORTABILIDAD_EFECTIVA=int,FLG_SANTANDER_PLUS=int,PORTA_DISP_M1=double,PORTA_DISP_M2=double,PORTA_DISP_M3=double,BANCO=string,FLG_SANTANDER_PLUS_CALIDAD=int,DES_RENTA=string,DES_SEGMENTO_ESP=string,DESC_SEGMENTO_AGRUPADO=string,SDO_CAPTACION=double,SDO_COLOCACION=double"
                                                                 # $15
# ************************* CONEXION A ORACLE (DESTINO DE LOS DATOS) ***************************************************

user_dest="usrstg"                                               # $16
driver="oracle.jdbc.driver.OracleDriver"                         # $17
password_dest="Secreta01"                                        # $18
url_jdbc_dest="jdbc:oracle:thin:@180.181.169.83:1660:pdmanltc"   # $19
table_dest="GESTION_CLIENTES"                                    # $20

# **************************** PATH DEL SHELL DE SPARK Y LIBRERIAS NESESARIAS ******************************************
shell_spark='./sh/spark-shell.sh'
library_hive='scala/header.scala'

################################# EXPORTAR LA TABLA ORACLE A HDFS      #################################################

./sh/export_oracle_to_hdfs.sh ${stored_directory_hdfs} ${processing_to_hdfs} ${exported_tables} ${files_java} ${url_jdbc} ${user} ${password} ${table} ${columns} ${path_hdfs_hive} ${schema_hive} ${table_hive} ${split_by} ${number_mappers} ${data_type_columns}


############## GENERAR EL CODIGO PARA ALMACENAR EN HDFS Y TAMBIEN PARA ENVIAR A ORACLE NUEVAMENTE #####################

sh/save_hdfs_and_write_jdbc.sh ${table} ${schema_hive} ${stored_directory_hdfs} ${processing_to_hdfs} ${exported_tables} ${user_dest} ${driver} ${password_dest} ${url_jdbc_dest} ${table_dest}

##########################  INVOCAR EL SHELL DE SPARK PARA ENVIAR LOS DATOS DE HDFS A ORACLE. ##########################

cat scala/header.scala ${processing_to_hdfs} | ./sh/spark-shell.sh

########### ELIMINAR LA TABLA TEMPORAL EN HIVE #########################################################################

./sh/drop_table_hive.sh ${schema_hive}"."${table_hive}

#########################  Remover ficheros temporales        #########################################################

remove_all_files_temp "${processing_to_hdfs};${exported_tables};${files_java}"
print_message "Script finalizado"





