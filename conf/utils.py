class Utilerias:
    """
    Params:
    init.scala         file with commands scala for partition created and data save
    tables_oracle.txt  contain all tables exported from oracle to hdfs
    """
    filename_command_scala = "../init.scala"
    tables_exported = "tables_oracle.txt"

    def __init__(self, scala_commands, tables_exported):
        """
        :param filename_command_scala This file contains any scala commands will call into spark-shell:
        :param tables_exported: This file contains all table oracle exported
        """

        self.filename_command_scala = scala_commands
        self.tables_exported = tables_exported
