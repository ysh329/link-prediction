# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_read_csv_input_data_save_word_to_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-11-09 13:48:08
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import MySQLdb
import time
import csv
from pyspark import SparkContext, SparkConf
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool
################################### PART2 CLASS && FUNCTION ###########################
class CSVDataSaver(object):
    def __init__(self, database_name):
        self.start = time.clock()

        logging.basicConfig(level = logging.INFO,
                  format = '%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s',
                  datefmt = '%y-%m-%d %H:%M:%S',
                  filename = './main.log',
                  filemode = 'a')
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s  %(levelname)5s %(filename)19s[line:%(lineno)3d] %(funcName)s %(message)s')
        console.setFormatter(formatter)

        logging.getLogger('').addHandler(console)
        logging.info("START CLASS {class_name}.".format(class_name = CSVDataSaver.__name__))

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def __del__(self):
        # close database
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        logging.info("END CLASS {class_name}.".format(class_name = CSVDataSaver.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CSVDataSaver.__name__, delta_time = self.end))



    def compute_node_in_different_network(self):
        pass

################################### PART3 CLASS TEST ##################################
"""
# Initialization
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
network_name_list = ["../data/input/Bio-network_directed.csv", "../data/input/Bio-network_undirected.csv",\
                     "../data/input/Info-network_directed.csv", "../data/input/Info-network_undirected.csv",\
                     "../data/input/Social network_directed.csv", "../data/input/Social network_undirected.csv"]

network_type_list = ["bio", "bio",\
                     "info", "info",\
                     "social", "social"]

is_directed_list = [0, 1,\
                    0, 1,\
                    0, 1]

CSVReader = CSVDataSaver(database_name = database_name)
CSVReader.read_csv_to_database(database_name = database_name,\
                               connection_table_name = connection_table_name,\
                               network_name_list = network_name_list,\
                               network_type_list = network_type_list,\
                               is_directed_list = is_directed_list)
"""