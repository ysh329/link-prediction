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



    def read_csv_to_database(self, database_name, connection_table_name, network_name_list, network_type_list, is_directed_list):
        # Get nodes from csv file
        # sub-function
        def get_node_pair_node_tuple_list(network_name):
            with open(network_name,'r') as f:
                reader = csv.reader(f)
                node_pair_tuple_list = map(lambda node_pair_list: (node_pair_list[0], node_pair_list[1]), reader)
                node_pair_tuple_list = map(lambda (node1, node2): (int(node1), int(node2)), node_pair_tuple_list[1:])
                logging.info("len(tuple_list):{0}".format(len(node_pair_tuple_list)))
            return node_pair_tuple_list

        node_pair_tuple_2d_list = []
        for network_idx in xrange(len(network_name_list)):
            network_name = network_name_list[network_idx]
            node_pair_tuple_list = get_node_pair_node_tuple_list(network_name = network_name)

            try:
                node_pair_tuple_2d_list.append(node_pair_tuple_list)
            except Exception as e:
                logging.error(e)
        logging.info("len(node_pair_tuple_2d_list):{0}".format(len(node_pair_tuple_2d_list)))

        # Generate sql
        # sub-function
        def insert_sql_generator(node_pair_tuple, network_type, is_directed, connection_id, database_name, connection_table_name):
            node1 = node_pair_tuple[0]
            node2 = node_pair_tuple[1]
            sql = """INSERT INTO {database_name}.{table_name}
                     (network_type, is_directed, connection_id,node1, node2)
                     VALUES
                     ('{network_type}', {is_directed}, {connection_id}, {node1}, {node2})"""\
            .format(database_name = database_name,\
                    table_name = connection_table_name,\
                    network_type = network_type,\
                    is_directed = is_directed,\
                    connection_id = connection_id,\
                    node1 = node1,\
                    node2 = node2,\
                    )
            return sql

        insert_sql_2d_list = []
        for network_idx in xrange(len(node_pair_tuple_2d_list)):
            node_pair_tuple_list = node_pair_tuple_2d_list[network_idx]
            network_type = network_type_list[network_idx]
            is_directed = is_directed_list[network_idx]
            try:
                """
                insert_sql_list = map(lambda node_pair_tuple: insert_sql_generator(node_pair_tuple = node_pair_tuple,\
                                                                                   network_type = network_type,\
                                                                                   is_directed = is_directed,\
                                                                                   connection_id = connection_id,\
                                                                                   database_name = database_name,\
                                                                                   connection_table_name = connection_table_name,\
                                                                                   ),\
                                      node_pair_tuple_list\
                                      )
                """
                insert_sql_list = []
                connection_id = 0
                for node_pair_idx in xrange(len(node_pair_tuple_list)):
                    connection_id = connection_id + 1
                    node_pair_tuple = node_pair_tuple_list[node_pair_idx]
                    sql = insert_sql_generator(node_pair_tuple = node_pair_tuple,\
                                               network_type = network_type,\
                                               is_directed = is_directed,\
                                               connection_id = connection_id,\
                                               database_name = database_name,\
                                               connection_table_name = connection_table_name,\
                                               )
                    insert_sql_list.append(sql)

                logging.info("len(insert_sql_list):{0}".format(len(insert_sql_list)))
                logging.info("insert_sql_list[0]:{0}".format(insert_sql_list[0]))
                insert_sql_2d_list.append(insert_sql_list)
            except Exception as e:
                logging.error("generate sql failed for network_idx:{0}".format(network_idx))
                logging.error(e)

        logging.info("len(insert_sql_2d_list):{0}".format(len(insert_sql_2d_list)))
        logging.info("len(insert_sql_2d_list[0]):{0}".format(len(insert_sql_2d_list[0])))
        logging.info("insert_sql_2d_list[0][:10]:{0}".format(insert_sql_2d_list[0][:10]))

        # Execute sql
        cursor = self.con.cursor()
        success_insert = 0
        failure_insert = 0
        for network_idx in xrange(len(insert_sql_2d_list)):
            insert_sql_list = insert_sql_2d_list[network_idx]
            insert_sql_list_length = len(insert_sql_list)
            for sql_idx in xrange(len(insert_sql_list)):
                if (sql_idx % 1000 == 0 and sql_idx > 998) or (sql_idx == insert_sql_list_length-1):
                    logging.info("============== inserting network idx:{0}==============".format(network_idx))
                    logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx = sql_idx, rate = float(sql_idx+1)/insert_sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_insert / float(success_insert + failure_insert)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_insert, failure = failure_insert))
                sql = insert_sql_list[sql_idx]
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_insert = success_insert + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    failure_insert = failure_insert + 1
        cursor.close()


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