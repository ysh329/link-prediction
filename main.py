# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: main.py
# Description:

# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-05 17:59:27
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
from mypackage.class_initialization_and_load_parameter import *
from mypackage.class_create_database_table import *
from mypackage.class_read_csv_input_data_save_to_database import *
################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    config_data_dir = "./config.ini"

    ParameterLoader = InitializationAndLoadParameter()

    network_name_list, network_type_list, is_directed_list,\
    database_name, connection_table_name, node_table_name = ParameterLoader\
        .load_parameter(config_data_dir = config_data_dir)



    # class_create_database_table
    Creater = CreateDatabaseTable()
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         connection_table_name = connection_table_name,\
                         node_table_name = node_table_name)



    # class_read_csv_input_data_save_to_database
    CSVReader = CSVDataSaver(database_name = database_name)
    CSVReader.read_csv_to_database(database_name = database_name,\
                                   connection_table_name = connection_table_name,\
                                   network_name_list = network_name_list,\
                                   network_type_list = network_type_list,\
                                   is_directed_list = is_directed_list)
################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()