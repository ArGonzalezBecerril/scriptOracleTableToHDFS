# Script que exporta una tabla de Oracle a
HDFS
Descripción: El siguiente script tiene como finalidad exportar una tabla de Oracle a HDFS con la cualidad que se
comprime en formato parquet y se particiona la información por año y mes.
[![N|Solid](https://trello-
attachments.s3.amazonaws.com/5a1baed8137a75335503cc3a/5afcd584aa5675f32966baf0/9aedf7c3b929d88d5eafa31d6a7aa0ad
/oracletoHdsf.PNG)](https://nodesource.com/products/nsolid)
Directorios
- **conf:** Contiene una clase escrita en python para poder conectarnos a la base de datos Oracle, adicionalmente se
debe agregar las columnas que necesitamos exportar y el directorio HDFS donde vamos a almacenar la
información
- **lib:** Librerías necesarios para la ejecución del script
- **logs:** Log que genera el script en cada ejecución tomando como base la fecha de su ejecución, en caso de que el
 script se invoque más de una vez al día se agregara en el mismo archivo el trace del script
- **scala:** Códigos en scala necesarios para el script, para esta versión solo se agregan algunos imports para crear el
 contexto en Hive.
- **sh:** Módulos escritos en bash el cual realizan tareas pequeñas como eliminar una tabla en hive o invocar un Shell
 de spark cada uno de estos programas son invocados por el script principal.

## Invocar el script principal
```sh
arturo@arturo$ ./run.sh
```

> **Instalación**
> Asegurarse de tener los permisos de ejecución,
> El script no nesesita de modulos adicionales ya que es out-of-the-box

Licencia
**BSD**
Mayo 23 2018