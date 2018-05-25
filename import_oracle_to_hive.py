from conf.connections import Conexion
from conf.utils import Utilerias
import sys
import getopt
import subprocess

'''
Version: 0.1
Author: Arturo Gonzalez

Descripcion : Programa que invoca un shell para exportar datos de oracle a hive

El script nesesita los siguientes argumentos


urlJDBC     -- Path completo jdbc por ejemplo jdbc:oracle:thin:@ip:puerto:DB
usuario     -- Nombre de Usuario para el acceso a la base de datos
contrasenia -- Constrasenia del Usuario de base de datos.
nombreDeTablaORacle
            -- Nombre de la tabla en oracle que se desea exportar a hive         
columnas    -- Nombre de todas las columnas en la tabla oracle que se desean exportar
dirHDFS     -- Directorio HDFS en hive donde se desea depositar la tabla de oracle
indicadorDeNuevoRegistro
            -- Especificarle a sqoop que tomara como separador para crear una nueva fila. Normalmente se agrega el id de
               la tabla ya que cada que se encuentre un id diferente se tomara como una nueva fila o registro.            
nombreTablaHive
             -- Se recomienda que sea el mismo que tiene de origen (Importante el nombre deve ir acompaniado del esquema 
               por ejemplo miEsquemaHive.NombreDeLaTablaHive             
numeroDeMappers
             -- Indica a sqoop cuantos procesos mapper utilizara para realizar la ejecucion del job.           

'''


def build_sqoop_script(conn,table):
    """
    :String url_jdbc: Path of jdbc
    :String user: Name of user into data base oracle
    :String password: Password of user oracle
    :String table_name_oracle: Table name oracle
    :String columns: Add the columns to import
    :String path_hdfs:Path hdfs output
    :String split_by : Indicator of new row
    :String schema hive: Name of data base where allocated table hive
    :Int number_mappers: Number of mappers for map-reduce process
    """
    sqoop_script = "sqoop import --connect " + conn.url_jdbc + " --username " + conn.user + " --password " + conn.password \
                   + " --table " + table + " --columns " + conn.columns + " --map-column-hive " \
                   + conn.data_type_columns + " --warehouse-dir " + conn.path_hdfs + " --split-by " + conn.split_by \
                   + "  --hive-import --hive-table " + conn.schema_hive + "." + table + " --num-mappers " \
                   + conn.number_mappers
    return sqoop_script


def run_script(sqoop_script):
    """
    :param sqoop_script: Is a command will run into terminal
    :return output command:
    """
    output_command = subprocess.Popen([sqoop_script], shell=True, stdout=subprocess.PIPE).communicate()[0]
    return output_command


def build_init_scala(table, conn, util):
    """
    :param table: Table name from oracle exported
    :param conn:  Configuration of connection pool
    :param util: Class with filename temporary
    :return:
    """
    build_init = 'cd sh && ./connection.sh ' + table + ' ' + conn.schema_hive + ' ' + conn.path_hdfs_data_partition + \
                 ' ' + util.filename_command_scala + ' ' + util.tables_exported
    is_build_init = subprocess.Popen([build_init], shell=True, stdout=subprocess.PIPE).communicate()[0]
    print(str(is_build_init))


def is_attribute_empty(conn):
    parameters_connection = [conn.url_jdbc, conn.user, conn.password, conn.table_name_oracle, conn.columns, conn.data_type_columns,
                             conn.path_hdfs, conn.split_by, conn.schema_hive, conn.number_mappers]
    if '' in parameters_connection:
        print("Existe por lo menos un parametro vacio, validar los atributos de conexion")
        exit(2)
    return False


def drop_table_hive_temp(conn, table):
    is_deleted_table = subprocess.Popen(['cd sh && ./drop_table_hive.sh ' + conn.schema_hive + '.' + table],
                                        shell=True, stdout=subprocess.PIPE).communicate()[0]
    print(str(is_deleted_table))


# *************************************MAIN*****************************************************************************
# Get new connection
connection = Conexion
util = Utilerias


is_empty = is_attribute_empty(connection)
tables_oracle = connection.table_name_oracle.split(",")

for table in tables_oracle:
    script_sqoop = build_sqoop_script(connection, table)
    run_script(script_sqoop)
    build_init_scala(table, connection, util)
    #print(script_sqoop)


# **********************************************************************************************************************
