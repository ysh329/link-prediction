# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_compute_link_similarity.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-13 10:16:24
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import MySQLdb
import time
from operator import add
import networkx as nx
from Tkinter import _flatten
################################### PART2 CLASS && FUNCTION ###########################
class ComputeLinkSimilarity(object):
    def __init__(self, database_name, pyspark_sc):
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
        logging.info("START CLASS {class_name}.".format(class_name = ComputeLinkSimilarity.__name__))

        # connect database
        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', db = database_name, charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        # spark configure
        try:
            self.sc = pyspark_sc
            logging.info("Config spark successfully.")
        except Exception as e:
            logging.error("Config spark failed.")
            logging.error(e)



    def __del__(self):
        # close database
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

        logging.info("END CLASS {class_name}.".format(class_name = ComputeLinkSimilarity.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = ComputeLinkSimilarity.__name__, delta_time = self.end))



    def get_node_and_connection_data_from_database(self, database_name, node_table_name, connection_table_name):
        cursor = self.con.cursor()
        # node table
        sqls = ["SELECT network_type, is_directed, node,"
                " degree_str, in_degree_str, out_degree_str"
                " FROM {database}.{table}"\
                .format(database = database_name,\
                       table = node_table_name)]
        # connection table
        sqls.append("SELECT network_type, is_directed,"
                    " node1, node2, common_neighbor_str, common_neighbor_num"
                    " FROM {database}.{table}"\
                    .format(database = database_name,\
                            table = connection_table_name))
        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == 0:
                    node_tuple_list = map(lambda (network_type,\
                                                     is_directed,\
                                                     node,\
                                                     degree_str,\
                                                     in_degree_str,\
                                                     out_degree_str): (str(network_type),\
                                                                       int(is_directed),\
                                                                       int(node),\
                                                                       str(degree_str),\
                                                                       str(in_degree_str),\
                                                                       str(out_degree_str)\
                                                                       ),\
                                          cursor.fetchall()\
                                          )
                    logging.info("len(node_tuple_list):{0}".format(len(node_tuple_list)))
                    logging.info("node_tuple_list[:3]:{0}".format(node_tuple_list[:3]))
                elif sql_idx == 1:
                    connection_tuple_list = map(lambda (network_type,\
                                                           is_directed,\
                                                           node1,\
                                                           node2,\
                                                           common_neighbor_str,\
                                                           common_neighbor_num): (str(network_type),\
                                                                                  int(is_directed),\
                                                                                  int(node1),\
                                                                                  int(node2),\
                                                                                  str(common_neighbor_str) if type(common_neighbor_str) == unicode else None,\
                                                                                  None if common_neighbor_num == None else int(common_neighbor_num)\
                                                                                  ),\
                                                cursor.fetchall()\
                                                )
                    logging.info("len(connection_tuple_list):{0}".format(len(connection_tuple_list)))
                    logging.info("connection_tuple_list[:3]:{0}".format(connection_tuple_list[:3]))
            except MySQLdb.Error, e:
                logging.error("failed to get node and connection data from database.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()
        return node_tuple_list, connection_tuple_list






    def compute_similarity_based_on_common_neighbor(self, node_tuple_list, connection_tuple_list):
        pass


    def compute_similarity_based_on_salton_index(self, node_tuple_list, connection_tuple_list):
        def compute_network_based_on_salton_index(network_type, is_directed, connection_tuple_rdd):
            connection_tuple_rdd = connection_tuple_rdd.filter(lambda (_network_type,\
                                                                          _is_directed,\
                                                                          node1,\
                                                                          node2,\
                                                                          common_neighbor_str,\
                                                                          common_neighbor_num): _network_type == network_type and _is_directed == is_directed\
                                                               )\
                                                        .map(lambda (network_type,\
                                                                     is_directed,\
                                                                     node1,\
                                                                     node2,\
                                                                     common_neighbor_str,\
                                                                     common_neighbor_num): (node1,\
                                                                                            node2,\
                                                                                            common_neighbor_str,\
                                                                                            common_neighbor_num)\
                                                             )
            logging.info("connection_tuple_rdd.count():{0}".format(connection_tuple_rdd.count()))
            logging.info("connection_tuple_rdd.take(3):{0}".format(connection_tuple_rdd.take(3)))


        connection_tuple_rdd = self.sc.parallelize(connection_tuple_list)





################################### PART3 CLASS TEST ##################################

# Initialization
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
node_table_name = "node_table"

from pyspark import SparkContext
pyspark_sc = SparkContext()


LinkSimComputer = ComputeLinkSimilarity(database_name = database_name,\
                                   pyspark_sc = pyspark_sc)
node_tuple_list, connection_tuple_list = LinkSimComputer\
    .get_node_and_connection_data_from_database(database_name = database_name,\
                                                node_table_name = node_table_name,\
                                                connection_table_name = connection_table_name)