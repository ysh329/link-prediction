# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_read_csv_input_data_save_word_to_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-06 15:22:38
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import MySQLdb
import time
from operator import add
import networkx as nx
################################### PART2 CLASS && FUNCTION ###########################
class ComputeNodeProperty(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = ComputeNodeProperty.__name__))

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

        logging.info("END CLASS {class_name}.".format(class_name = ComputeNodeProperty.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = ComputeNodeProperty.__name__, delta_time = self.end))



    def read_connection_data_in_database(self, database_name, connection_table_name):
        cursor = self.con.cursor()
        sqls = []
        sqls.append("""USE {0}""".format(database_name))
        sqls.append("""SELECT network_type, is_directed, connection_id, node1, node2 FROM {database}.{table}"""\
                    .format(database = database_name, table = connection_table_name)\
                    )

        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == len(sqls)-1:
                    connection_data_2d_tuple = cursor.fetchall()
                    logging.info("len(connection_data_2d_tuple):{0}".format(len(connection_data_2d_tuple)))
                    logging.info("type(connection_data_2d_tuple):{0}".format(type(connection_data_2d_tuple)))
                    logging.info("connection_data_2d_tuple[:10]:{0}".format(connection_data_2d_tuple[:10]))
                    logging.info("type(connection_data_2d_tuple[0]):{0}".format(type(connection_data_2d_tuple[0])))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in attaining connection data from {database_name}.{table_name}.".format(database_name = database_name, table_name = connection_table_name))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                return 0
        cursor.close()

        # transform to rdd
        connection_data_tuple_rdd = self.sc.parallelize(connection_data_2d_tuple)\
                                           .map(lambda (network_type, is_directed, connection_id, node1, node2):\
                                                                                        (network_type.encode("utf8"),\
                                                                                         int(is_directed),\
                                                                                         int(connection_id),\
                                                                                         int(node1),\
                                                                                         int(node2))\
                                                )
        logging.info("connection_data_tuple_rdd.count():{0}".format(connection_data_tuple_rdd.count()))
        logging.info("connection_data_tuple_rdd.take(3):{0}".format(connection_data_tuple_rdd.take(3)))


        # transform to six different rdd
        ## bio network
        bio_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "bio" and is_directed == 0)
        logging.info("bio_directed_tuple_rdd.count():{0}".format(bio_directed_tuple_rdd.count()))
        logging.info("bio_directed_tuple_rdd.take(3):{0}".format(bio_directed_tuple_rdd.take(3)))

        bio_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "bio" and is_directed == 1)
        logging.info("bio_undirected_tuple_rdd.count():{0}".format(bio_undirected_tuple_rdd.count()))
        logging.info("bio_undirected_tuple_rdd.take(3):{0}".format(bio_undirected_tuple_rdd.take(3)))

        # info network
        info_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "info" and is_directed == 0)
        logging.info("info_directed_tuple_rdd.count():{0}".format(info_directed_tuple_rdd.count()))
        logging.info("info_directed_tuple_rdd.take(3):{0}".format(info_directed_tuple_rdd.take(3)))

        info_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "info" and is_directed == 1)
        logging.info("info_undirected_tuple_rdd.count():{0}".format(info_undirected_tuple_rdd.count()))
        logging.info("info_undirected_tuple_rdd.take(3):{0}".format(info_undirected_tuple_rdd.take(3)))

        # social network
        social_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "social" and is_directed == 0)
        logging.info("social_directed_tuple_rdd.count():{0}".format(social_directed_tuple_rdd.count()))
        logging.info("social_directed_tuple_rdd.take(3):{0}".format(social_directed_tuple_rdd.take(3)))

        social_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "social" and is_directed == 1)
        logging.info("social_undirected_tuple_rdd.take(3):{0}".format(social_undirected_tuple_rdd.count()))
        logging.info("social_undirected_tuple_rdd.take(3):{0}".format(social_undirected_tuple_rdd.take(3)))

        network_rdd_list = [bio_directed_tuple_rdd, bio_undirected_tuple_rdd,\
                            info_directed_tuple_rdd, info_undirected_tuple_rdd,\
                            social_directed_tuple_rdd, social_undirected_tuple_rdd]
        return network_rdd_list



    def compute_degree_in_different_network(self, network_rdd_list):
        bio_directed_tuple_rdd, bio_undirected_tuple_rdd = network_rdd_list[0], network_rdd_list[1]
        info_directed_tuple_rdd, info_undirected_tuple_rdd = network_rdd_list[2], network_rdd_list[3]
        social_directed_tuple_rdd, social_undirected_tuple_rdd = network_rdd_list[4], network_rdd_list[5]

        # sub-function
        def compute_degree_in_undirected_network(network_data_tuple_rdd):
            try:
                node_and_degree_tuple_rdd = network_data_tuple_rdd\
                    .map(lambda (network_type, is_directed, connection_id, node1, node2): (node1, node2))\
                    .flatMap(lambda node_tuple: [node_tuple[0], node_tuple[1]])\
                    .map(lambda node: (node, 1))\
                    .reduceByKey(add)\
                    .sortBy(lambda (node, degree): degree, False)

                logging.info("node_and_degree_tuple_rdd.count():{0}".format(node_and_degree_tuple_rdd.count()))
                logging.info("node_and_degree_tuple_rdd.take(3):{0}".format(node_and_degree_tuple_rdd.take(3)))
            except Exception as e:
                logging.error(e)
                return None
            return node_and_degree_tuple_rdd

        # sub-function
        def compute_degree_in_directed_network(network_data_tuple_rdd):
            try:
                node_tuple_rdd = network_data_tuple_rdd\
                    .map(lambda (network_type, is_directed, connection_id, node1, node2): (node1, node2))
                logging.info("node_tuple_rdd.count():{0}".format(node_tuple_rdd.count()))
                logging.info("node_tuple_rdd.take(3):{0}".format(node_tuple_rdd.take(3)))

                node_and_in_degree_rdd = node_tuple_rdd\
                    .map(lambda (node_in, node_out): (node_in, 1))\
                    .reduceByKey(add)
                logging.info("node_and_in_degree_rdd.count():{0}".format(node_and_in_degree_rdd.count()))
                logging.info("node_and_in_degree_rdd.take(3):{0}".format(node_and_in_degree_rdd.take(3)))

                node_and_out_degree_rdd = node_tuple_rdd\
                    .map(lambda (node_in, node_out): (node_out, 1))\
                    .reduceByKey(add)
                logging.info("node_and_out_degree_rdd.count():{0}".format(node_and_out_degree_rdd.count()))
                logging.info("node_and_out_degree_rdd.take(3):{0}".format(node_and_out_degree_rdd.take(3)))

                node_and_in_and_out_degree_tuple_rdd = node_and_in_degree_rdd\
                    .fullOuterJoin(node_and_out_degree_rdd)\
                    .map(lambda (node, (in_degree, out_degree)): (node, in_degree, out_degree))\
                    .sortBy(lambda (node, in_degree, out_degree): out_degree, False)
                logging.info("node_and_in_and_out_degree_tuple_rdd.count():{0}".format(node_and_in_and_out_degree_tuple_rdd.count()))
                logging.info("node_and_in_and_out_degree_tuple_rdd.take(3):{0}".format(node_and_in_and_out_degree_tuple_rdd.take(3)))
            except Exception as e:
                logging.error("Compute degree failed in directed network.")
                logging.error(e)
                return None
            return node_and_in_and_out_degree_tuple_rdd

        # sub-function
        def compute_node_num_in_network(network_data_tuple_rdd):
            try:
                node_and_degree_tuple_rdd = compute_degree_in_undirected_network(network_data_tuple_rdd)
                node_num_in_network = node_and_degree_tuple_rdd.count()
            except Exception as e:
                logging.error("Compute node number failed in network.")
                logging.error(e)
                return None
            return node_num_in_network, node_and_degree_tuple_rdd

        # sub-function
        def compute_normalized_degree_in_network(network_data_tuple_rdd):
            try:
                node_num_in_network, node_and_degree_tuple_rdd = compute_node_num_in_network(network_data_tuple_rdd)
                logging.info("node_num_in_network:{0}".format(node_num_in_network))
                logging.info("node_and_degree_tuple_rdd.take(3):{0}".format(node_and_degree_tuple_rdd.take(3)))

                node_and_normalized_degree_tuple_rdd = node_and_degree_tuple_rdd\
                    .map(lambda (node, degree): (node, float(degree) / node_num_in_network) )\
                    .sortBy(lambda (node, normalized_degree): normalized_degree, False)
                logging.info("node_and_normalized_degree_tuple_rdd.take(3):{0}".format(node_and_normalized_degree_tuple_rdd.take(3)))
            except Exception as e:
                logging.error("Compute normalized degree failed in network.")
                logging.error(e)
                return None
            return node_and_normalized_degree_tuple_rdd

        bio_undirected_node_and_degree_tuple_rdd = compute_degree_in_undirected_network(network_data_tuple_rdd = bio_undirected_tuple_rdd)
        info_directed_node_and_degree_tuple_rdd = compute_degree_in_directed_network(network_data_tuple_rdd = info_directed_tuple_rdd)

        bio_directed_node_and_normalized_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = bio_undirected_tuple_rdd)








################################### PART3 CLASS TEST ##################################
"""
# Initialization
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"

Computer = ComputeNodeProperty(database_name = database_name)
Computer.read_connection_data_in_database(database_name = database_name,\
                                          connection_table_name = connection_table_name)
"""