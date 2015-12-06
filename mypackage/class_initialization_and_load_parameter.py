# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_initialization_and_load_parameter.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-05 17:38:28
# Last:
__author__ = 'yuens'
################################### PART1 IMPORT ######################################
import logging
import ConfigParser
import time
################################### PART2 CLASS && FUNCTION ###########################
class InitializationAndLoadParameter(object):
    def __init__(self):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = 'main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = InitializationAndLoadParameter.__name__))



    def __del__(self):
        logging.info("Success in quiting MySQL.")
        logging.info("END CLASS {class_name}.".format(class_name = InitializationAndLoadParameter.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = InitializationAndLoadParameter.__name__, delta_time = self.end - self.start))



    def load_parameter(self, config_data_dir):
        conf = ConfigParser.ConfigParser()
        conf.read(config_data_dir)

        #[bio network]
        bio_network_directed_name = conf.get("data", "bio_network_directed_name")
        logging.info("bio_network_directed_name:{0}".format(bio_network_directed_name))

        bio_network_undirected_name = conf.get("data", "bio_network_undirected_name")
        logging.info("bio_network_undirected_name:{0}".format(bio_network_undirected_name))

        #[info network]
        info_network_directed_name = conf.get("data", "info_network_directed_name")
        logging.info("info_network_directed_name:{0}".format(info_network_directed_name))

        info_network_undirected_name = conf.get("data", "info_network_undirected_name")
        logging.info("info_network_undirected_name:{0}".format(info_network_undirected_name))

        #[social network]
        social_network_directed_name = conf.get("data", "social_network_directed_name")
        logging.info("social_network_directed_name:{0}".format(social_network_directed_name))

        social_network_undirected_name = conf.get("data", "social_network_undirected_name")
        logging.info("social_network_undirected_name:{0}".format(social_network_undirected_name))

        network_name_list = [bio_network_directed_name, bio_network_undirected_name,\
                             info_network_directed_name, info_network_undirected_name,\
                             social_network_directed_name, social_network_undirected_name]

        #[basic]
        database_name = conf.get("basic", "database_name")
        logging.info("database_name:{0}".format(database_name))

        connection_table_name = conf.get("basic", "connection_table_name")
        logging.info("connection_table_name:{0}".format(connection_table_name))

        node_table_name = conf.get("basic", "node_table_name")
        logging.info("node_table_name:{0}".format(node_table_name))

        network_type_list = conf.get("basic", "network_type_str").replace(" ", "").split(",")
        logging.info("network_type_list:{0}".format(network_type_list))

        is_directed_list = map(int, conf.get("basic", "is_directed_str").replace(" ", "").split(","))
        logging.info("is_directed_list:{0}".format(is_directed_list))

        pyspark_app_name = conf.get("basic", "pyspark_app_name")
        logging.info("pyspark_app_name:{0}".format(pyspark_app_name))

        return network_name_list, network_type_list, is_directed_list, database_name, connection_table_name, node_table_name, pyspark_app_name


################################### PART3 CLASS TEST ##################################
"""
# Initialization
config_data_dir = "../config.ini"

# load parameters
ParameterLoader = InitializationAndLoadParameter()

network_name_list, network_type_list, is_directed_list,\
database_name, connection_table_name, node_table_name = ParameterLoader\
    .load_parameter(config_data_dir = config_data_dir)
"""