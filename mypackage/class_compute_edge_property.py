# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_compute_meta_data_of_network.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-06 21:49:46
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
class ComputeEdgeProperty(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = ComputeEdgeProperty.__name__))

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

        logging.info("END CLASS {class_name}.".format(class_name = ComputeEdgeProperty.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = ComputeEdgeProperty.__name__, delta_time = self.end))



    def compute_common_degree_in_different_network(self, database_name, node_table_name, connection_table_name):
        # sub-function
        def get_node_and_connection_data_from_database(self, database_name, node_table_name, connection_table_name):
            cursor = self.con.cursor()
            sqls = ["SELECT network_type, is_directed, node,"
                    " degree_str, in_degree_str, out_degree_str"
                    " FROM {database}.{table}"\
                    .format(database = database_name,\
                           table = node_table_name)]

            sqls.append("SELECT network_type, is_directed, node1, node2"
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
                                                                           str(out_degree_str)),\
                                              cursor.fetchall()\
                                              )
                        logging.info("len(node_tuple_list):{0}".format(len(node_tuple_list)))
                        logging.info("node_tuple_list[:3]:{0}".format(node_tuple_list[:3]))
                    elif sql_idx == 1:
                        connection_tuple_list = map(lambda (network_type,\
                                                               is_directed,\
                                                               node1,\
                                                               node2): (str(network_type),\
                                                                        int(is_directed),\
                                                                        int(node1),\
                                                                        int(node2)\
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

        # sub-function
        def compute_common_degree_in_network(network_type, is_directed, node_tuple_list, connection_tuple_list):
            node_tuple_list = filter(lambda (network_type_,\
                                                is_directed_,\
                                                node,\
                                                degree_str,\
                                                in_degree_str,\
                                                out_degree_str): network_type_ == network_type and is_directed_ == is_directed,\
                                     node_tuple_list\
                                     )
            edge_tuple_list = filter(lambda (network_type_,\
                                                is_directed_,\
                                                node1,\
                                                node2): network_type_ == network_type and is_directed_ == is_directed,\
                                     connection_tuple_list\
                                     )
            edge_tuple_list_length = len(edge_tuple_list)
            success_compute = 0
            failure_compute = 0

            common_degree_str_list_in_network = []
            common_degree_num_list_in_network = []
            common_edge_tuple_list_in_network = []
            common_degree_rate_list_in_network = []

            for edge_idx in xrange(len(edge_tuple_list)):
                edge_tuple = edge_tuple_list[edge_idx]
                node1 = edge_tuple[2]
                node2 = edge_tuple[3]
                if (edge_idx % 1000 == 0 and edge_idx > 998) or (edge_idx == edge_tuple_list_length-1):
                    logging.info("============== Computer common node of {edge_idx}th edge in {network_type}.{is_directed} network ==============".format(edge_idx = edge_idx, network_type = network_type, is_directed = is_directed))
                    logging.info("edge_index:{idx}, finish rate:{rate}".format(idx = edge_idx, rate = float(edge_idx+1)/edge_tuple_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_compute / float(success_compute + failure_compute + 0.0001)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_compute, failure = failure_compute))


                try:
                    node1_and_degree_str_list = filter(lambda (network_type,\
                                                                  is_directed,\
                                                                  node,\
                                                                  degree_str,\
                                                                  in_degree_str,\
                                                                  out_degree_str): node == node1,\
                                                       node_tuple_list)[0]
                except Exception as e:
                    failure_compute = failure_compute + 1
                    logging.error(e)
                    continue

                if node1_and_degree_str_list[3] == "":
                    node1_degree_list = []
                else:
                    node1_degree_list = node1_and_degree_str_list[3].split("///")
                try:
                    node2_and_degree_str_list = filter(lambda (network_type,\
                                                                  is_directed,\
                                                                  node,\
                                                                  degree_str,\
                                                                  in_degree_str,\
                                                                  out_degree_str): node == node2,\
                                                       node_tuple_list)[0]
                except Exception as e:
                    failure_compute = failure_compute + 1
                    logging.error(e)
                    continue

                if node2_and_degree_str_list[3] == "":
                    node2_degree_list = []
                else:
                    node2_degree_list = node2_and_degree_str_list[3].split("///")

                # Merge current result
                node1_degree_list = _flatten([node1_degree_list])
                node2_degree_list = _flatten([node2_degree_list])

                if len(node2_degree_list) <= len(node1_degree_list):
                    common_degree_list = filter(lambda node: node in node1_degree_list, node2_degree_list)
                else:
                    common_degree_list = filter(lambda node: node in node2_degree_list, node1_degree_list)
                common_degree_str_list_in_network.append("///".join(map(str, common_degree_list)))
                common_degree_num_list_in_network.append(len(common_degree_list))
                common_edge_tuple_list_in_network.append((node1, node2))
                common_degree_rate_list_in_network.append(len(common_degree_list)/float(len(node1_degree_list)+len(node2_degree_list)))

                success_compute = success_compute + 1

            # Merge all results
            degree_data_tuple_list_in_network = map(lambda edge_tuple, degree_str, degree_num, degree_rate:\
                                                        (edge_tuple, degree_str, degree_num, degree_rate),\
                                                    common_edge_tuple_list_in_network,\
                                                    common_degree_str_list_in_network,\
                                                    common_degree_num_list_in_network,\
                                                    common_degree_rate_list_in_network\
                                                    )
            logging.info("len(degree_data_tuple_list_in_network):{0}".format(len(degree_data_tuple_list_in_network)))
            logging.info("degree_data_tuple_list_in_network[:3]:{0}".format(degree_data_tuple_list_in_network[:3]))
            return degree_data_tuple_list_in_network


        # sub-function
        def sql_generator(database_name, connection_table_name, network_type, is_directed, node1, node2, common_neighbor_str, common_neighbor_num, common_neighbor_rate):
            try:
                sql = """UPDATE {database_name}.{table_name}
                         SET common_neighbor_str = '{common_neighbor_str}',
                             common_neighbor_num = {common_neighbor_num},
                             common_neighbor_rate = {common_neighbor_rate}
                         WHERE network_type = '{network_type}' AND
                               is_directed = {is_directed} AND
                               node1 = {node1} AND
                               node2 = {node2}"""\
                    .format(database_name = database_name, table_name = connection_table_name,\
                            common_neighbor_str = common_neighbor_str, common_neighbor_num = common_neighbor_num, common_neighbor_rate = common_neighbor_rate,\
                            network_type = network_type, is_directed = is_directed, node1 = node1, node2 = node2)
            except Exception as e:
                logging.error("Failed to generate sql for edge{node1}-{node2} {type} network is_directed:{directed}."\
                              .format(node1 = node1,\
                                      node2 = node2,\
                                      type = network_type,\
                                      directed = is_directed)\
                              )
                logging.error(e)
            return sql

        # sub-function
        def execute_update_sql_for_database(self, network_type, is_directed, database_name, connection_table_table, sql_list):
            cursor = self.con.cursor()
            sql_list_length = len(sql_list)
            success_update = 0
            failure_update = 0
            for sql_idx in xrange(len(sql_list)):
                sql = sql_list[sql_idx]
                if (sql_idx % 1000 == 0 and sql_idx > 998) or (sql_idx == sql_list_length-1):
                    logging.info("============== update {idx}th sql for {network_type}.{is_directed} network ==============".format(idx = sql_idx, network_type = network_type, is_directed = is_directed))
                    logging.info("sql_index:{idx}, finish rate:{rate}".format(idx = sql_idx, rate = float(sql_idx+1)/sql_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_update / float(success_update + failure_update + 0.0001)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))
                try:
                    cursor.execute(sql)
                    self.con.commit()
                    success_update = success_update + 1
                except MySQLdb.Error, e:
                    self.con.rollback()
                    logging.error("Failed in update {database}.{table} in MySQL.".format(database = database_name, table = connection_table_name))
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    logging.error(sql)
                    failure_update = failure_update + 1
                    continue

        # start
        # get data from database
        node_tuple_list, connection_tuple_list = get_node_and_connection_data_from_database(self = self,\
                                                                                            database_name = database_name,\
                                                                                            node_table_name = node_table_name,\
                                                                                            connection_table_name = connection_table_name)
        '''
        # bio un
        bio_undirected_common_node_list = compute_common_degree_in_network(network_type = "bio",\
                                                                           is_directed = 0,\
                                                                           node_tuple_list = node_tuple_list,\
                                                                           connection_tuple_list = connection_tuple_list)
        bio_undirected_update_sql_list = map(lambda ((node1,\
                                                     node2),\
                                                     common_neighbor_str,\
                                                     common_neighbor_num,\
                                                     common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "bio",\
                                                               is_directed = 0,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             bio_undirected_common_node_list,\
                                             )
        logging.info("len(bio_undirected_update_sql_list:{0}".format(len(bio_undirected_update_sql_list)))
        logging.info("bio_undirected_update_sql_list[:3]:{0}".format(bio_undirected_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "bio",\
                                        is_directed = 0,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = bio_undirected_update_sql_list)


        # bio di
        bio_directed_common_node_list = compute_common_degree_in_network(network_type = "bio",\
                                                                           is_directed = 1,\
                                                                           node_tuple_list = node_tuple_list,\
                                                                           connection_tuple_list = connection_tuple_list)
        bio_directed_update_sql_list = map(lambda ((node1,\
                                                     node2),\
                                                     common_neighbor_str,\
                                                     common_neighbor_num,\
                                                     common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "bio",\
                                                               is_directed = 1,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             bio_directed_common_node_list,\
                                             )
        logging.info("len(bio_directed_update_sql_list:{0}".format(len(bio_directed_update_sql_list)))
        logging.info("bio_directed_update_sql_list[:3]:{0}".format(bio_directed_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "bio",\
                                        is_directed = 1,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = bio_directed_update_sql_list)


        # info un
        info_undirected_common_node_list = compute_common_degree_in_network(network_type = "info",\
                                                                            is_directed = 0,\
                                                                            node_tuple_list = node_tuple_list,\
                                                                            connection_tuple_list = connection_tuple_list)
        info_undirected_update_sql_list = map(lambda ((node1,\
                                                      node2),\
                                                      common_neighbor_str,\
                                                      common_neighbor_num,\
                                                      common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "info",\
                                                               is_directed = 0,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             info_undirected_common_node_list,\
                                             )
        logging.info("len(info_undirected_update_sql_list:{0}".format(len(info_undirected_update_sql_list)))
        logging.info("info_undirected_update_sql_list[:3]:{0}".format(info_undirected_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "info",\
                                        is_directed = 0,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = info_undirected_update_sql_list)


        # info di
        info_directed_common_node_list = compute_common_degree_in_network(network_type = "info",\
                                                                            is_directed = 1,\
                                                                            node_tuple_list = node_tuple_list,\
                                                                            connection_tuple_list = connection_tuple_list)
        info_directed_update_sql_list = map(lambda ((node1,\
                                                      node2),\
                                                      common_neighbor_str,\
                                                      common_neighbor_num,\
                                                      common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "info",\
                                                               is_directed = 1,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             info_directed_common_node_list,\
                                             )
        logging.info("len(info_directed_update_sql_list:{0}".format(len(info_directed_update_sql_list)))
        logging.info("info_directed_update_sql_list[:3]:{0}".format(info_directed_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "info",\
                                        is_directed = 1,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = info_directed_update_sql_list)


        # social un
        social_undirected_common_node_list = compute_common_degree_in_network(network_type = "social",\
                                                                            is_directed = 0,\
                                                                            node_tuple_list = node_tuple_list,\
                                                                            connection_tuple_list = connection_tuple_list)
        social_undirected_update_sql_list = map(lambda ((node1,\
                                                      node2),\
                                                      common_neighbor_str,\
                                                      common_neighbor_num,\
                                                      common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "social",\
                                                               is_directed = 0,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             social_undirected_common_node_list,\
                                             )
        logging.info("len(social_undirected_update_sql_list:{0}".format(len(social_undirected_update_sql_list)))
        logging.info("social_undirected_update_sql_list[:3]:{0}".format(social_undirected_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "social",\
                                        is_directed = 0,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = social_undirected_update_sql_list)
        '''


        # social di
        social_directed_common_node_list = compute_common_degree_in_network(network_type = "social",\
                                                                            is_directed = 1,\
                                                                            node_tuple_list = node_tuple_list,\
                                                                            connection_tuple_list = connection_tuple_list)
        social_directed_update_sql_list = map(lambda ((node1,\
                                                      node2),\
                                                      common_neighbor_str,\
                                                      common_neighbor_num,\
                                                      common_neighbor_rate):\
                                                 sql_generator(database_name = database_name,\
                                                               connection_table_name = connection_table_name,\
                                                               network_type = "social",\
                                                               is_directed = 1,\
                                                               node1 = node1,\
                                                               node2 = node2,\
                                                               common_neighbor_str = common_neighbor_str,\
                                                               common_neighbor_num = common_neighbor_num,\
                                                               common_neighbor_rate = common_neighbor_rate\
                                                               ),\
                                             social_directed_common_node_list,\
                                             )
        logging.info("len(social_directed_update_sql_list:{0}".format(len(social_directed_update_sql_list)))
        logging.info("social_directed_update_sql_list[:3]:{0}".format(social_directed_update_sql_list[:3]))
        execute_update_sql_for_database(self = self,\
                                        network_type = "social",\
                                        is_directed = 1,\
                                        database_name = database_name,\
                                        connection_table_table = connection_table_name,\
                                        sql_list = social_directed_update_sql_list)

################################### PART3 CLASS TEST ##################################
'''
# Initialization
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
node_table_name = "node_table"

from pyspark import SparkContext
pyspark_sc = SparkContext()


EdgeComputer = ComputeEdgeProperty(database_name = database_name,\
                                   pyspark_sc = pyspark_sc)
EdgeComputer.compute_common_degree_in_different_network(database_name = database_name,\
                                                        node_table_name = node_table_name,\
                                                        connection_table_name = connection_table_name)
'''