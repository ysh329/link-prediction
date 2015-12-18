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
from mypackage.class_create_spark import *
from mypackage.class_compute_node_property import *
from mypackage.class_compute_edge_property import *
from mypackage.class_data_collation_for_database import *
################################ PART3 MAIN ###########################################
def main():
    # class_initialization_and_load_parameter
    config_data_dir = "./config.ini"

    ParameterLoader = InitializationAndLoadParameter()

    network_name_list, network_type_list, is_directed_list,\
    database_name, connection_table_name, node_table_name,\
    pyspark_app_name = ParameterLoader\
        .load_parameter(config_data_dir = config_data_dir)



    # class_create_database_table
    Creater = CreateDatabaseTable()
    Creater.create_database(database_name = database_name)
    Creater.create_table(database_name = database_name,\
                         connection_table_name = connection_table_name,\
                         node_table_name = node_table_name)


    '''
    # class_read_csv_input_data_save_to_database
    CSVReader = CSVDataSaver(database_name = database_name)
    CSVReader.read_csv_to_database(database_name = database_name,\
                                   connection_table_name = connection_table_name,\
                                   network_name_list = network_name_list,\
                                   network_type_list = network_type_list,\
                                   is_directed_list = is_directed_list)
    '''


    # class_create_spark
    SparkCreator = CreateSpark(pyspark_app_name = pyspark_app_name)
    pyspark_sc = SparkCreator.return_spark_context()


    '''
    # class_compute_node_property
    # degree_str, degree_num, in_degree_str, in_degree_num, out_degree_str, out_degree_num
    # in_degree_rate, normalized_degree
    Computer = ComputeNodeProperty(database_name = database_name, pyspark_sc = pyspark_sc)

    network_rdd_list = Computer.read_connection_data_in_database(database_name = database_name,\
                                                                 connection_table_name = connection_table_name)
    node_data_rdd_list = Computer.compute_degree_in_different_network(network_rdd_list = network_rdd_list)
    Computer.save_node_data_rdd_list_to_database(database_name = database_name,\
                                                 node_table_name = node_table_name,\
                                                 node_data_rdd_list = node_data_rdd_list)
    '''


    '''
    # class_compute_edge_property
    EdgeComputer = ComputeEdgeProperty(database_name = database_name,\
                                       pyspark_sc = pyspark_sc)
    EdgeComputer.compute_common_degree_in_different_network(database_name = database_name,\
                                                            node_table_name = node_table_name,\
                                                            connection_table_name = connection_table_name)
    '''


    # class_data_collation_for_database
    DC = DataCollationInDB()
    node_tuple_list, connection_tuple_list = DC\
        .get_data_from_database(database_name = database_name,\
                                node_table_name = node_table_name,\
                                connection_table_name = connection_table_name)
    DC.sql_generator_for_all_connection(database_name = database_name,\
                                        connection_table_name = connection_table_name,\
                                        node_tuple_list = node_tuple_list,\
                                        connection_tuple_list = connection_tuple_list)
################################ PART4 EXECUTE ##################################
if __name__ == "__main__":
    main()