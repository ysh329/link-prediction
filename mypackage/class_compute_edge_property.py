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
        def compute_common_degree_in_network(network_type, is_directed):
            pass

        # sub-function
        def create_graph_for_network(network_type, is_directed, connection_tuple_list):
            try:
                edge_tuple_list = filter(lambda (network_type_,\
                                                    is_directed_,\
                                                    node1,\
                                                    node2): network_type_ == network_type and is_directed_ == is_directed,\
                                         connection_tuple_list)
                logging.info("len(edge_tuple_list):{0}".format(len(edge_tuple_list)))
                logging.info("edge_tuple_list[:3]):{0}".format(edge_tuple_list[:3]))
                if is_directed == 1:
                    G = nx.DiGraph()
                else:
                    G = nx.Graph()
                map(lambda (network_type, is_directed, node1, node2): G.add_edge(node1, node2), edge_tuple_list)
                logging.info("G:{0}".format(G))
            except Exception as e:
                logging.error("Failed to create graph for {network_type} directed:{is_directed} network".format(network_type = network_type, is_directed = is_directed))
                logging.error(e)
                return None
            return G

        # sub-function
        def find_common_node_in_graph(G, network_type, is_directed, node_tuple_list):
            common_neighbor_num_list = []

            node_num = len(G.nodes())
            edge_tuple_list = G.edges()
            logging.info("G.nodes()[:3]:{0}".format(G.nodes()[:3]))
            logging.info("node_num:{0}".format(node_num))
            logging.info("len(edge_tuple_list)".format(len(edge_tuple_list)))
            logging.info("edge_tuple_list[:3]:{0}".format(edge_tuple_list[:3]))

            success_compute = 0
            failure_compute = 0
            node_and_neighbor_node_list_tuple_list = map(lambda node: (node, G.neighbors(node)), G.nodes())
            edge_tuple_list_length = len(edge_tuple_list)

            for edge_idx in xrange(len(edge_tuple_list)):
                edge_tuple = edge_tuple_list[edge_idx]
                node1 = edge_tuple[0]
                node2 = edge_tuple[1]
                if (edge_idx % 1000 == 0 and edge_idx > 998) or (edge_idx == edge_tuple_list_length-1):
                    logging.info("============== Computer common node of {edge_idx}th edge in {network_type}.{is_directed} network==============".format(edge_idx = edge_idx, network_type = network_type, is_directed = is_directed))
                    logging.info("edge_index:{idx}, finish rate:{rate}".format(idx = edge_idx, rate = float(edge_idx+1)/edge_tuple_list_length))
                    logging.info("success_rate:{success_rate}".format(success_rate = success_compute / float(success_compute + failure_compute + 0.0001)))
                    logging.info("success_update:{success}, failure_update:{failure}".format(success = success_compute, failure = failure_compute))
                try:
                    node1_and_node1_neighbor_list_tuple = filter(lambda (node, neighbor_node_list): node == node1, node_and_neighbor_node_list_tuple_list)[0]
                    node1_neighbor_list = node1_and_node1_neighbor_list_tuple[1]

                    node2_and_node2_neighbor_list_tuple = filter(lambda (node, neighbor_node_list): node == node2, node_and_neighbor_node_list_tuple_list)[0]
                    node2_neighbor_list = node2_and_node2_neighbor_list_tuple[1]

                    if len(node1_neighbor_list) <= len(node2_neighbor_list):
                        common_neighbor_num = map(lambda node1_neighbor_node: node1_neighbor_node in node2_neighbor_list, node1_neighbor_list).count(True)
                    else:
                        common_neighbor_num = map(lambda node2_neighbor_node: node2_neighbor_node in node1_neighbor_list, node2_neighbor_list).count(True)
                    common_neighbor_num_list.append(common_neighbor_num)
                    success_compute = success_compute + 1
                except Exception as e:
                    logging.error("Failed to compute common node for {network}.{is_directed} for edge{edge}.".format(network = network_type, is_directed = is_directed), edge = edge_tuple)
                    logging.error(e)
                    failure_compute = failure_compute + 1
                    continue
            edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list = map(lambda (node1, node2), common_neighbor_num: (node1,\
                                                                                                                                node2,\
                                                                                                                                common_neighbor_num,\
                                                                                                                                float(common_neighbor_num)/node_num),\
                                                                                   edge_tuple_list, common_neighbor_num_list\
                                                                                   )
            logging.info("len(edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list):{0}".format(len(edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list)))
            logging.info("edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list[:3]:{0}".format(edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list[:3]))

            return edge_and_common_neighbor_num_and_common_neighbor_rate_tuple_list

        # sub-function
        def sql_generator(database_name, connection_table_name, network_type, is_directed, node1, node2, common_neighbor_str, common_neighbor_num, common_neighbor_rate):
            try:
                sql = """UPDATE {database_name}.{table_name}
                         SET common_neighbor_str = '{common_neighbor_str}',
                             common_neighbor_num = {common_neighbor_num},
                             common_neighbor_rate = {common_neighbor_rate}
                         WHERE network_type = '{network_type}',
                               is_directed = {is_directed},
                               node1 = {node1},
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



        node_tuple_list, connection_tuple_list = get_node_and_connection_data_from_database(self = self,\
                                                                                            database_name = database_name,\
                                                                                            node_table_name = node_table_name,\
                                                                                            connection_table_name = connection_table_name)

        #bio_directed_graph = create_graph_for_network(network_type = "bio", is_directed = 1, connection_tuple_list = connection_tuple_list)

        bio_undirected_graph = create_graph_for_network(network_type = "bio", is_directed = 0, connection_tuple_list = connection_tuple_list)
        info_undirected_graph = create_graph_for_network(network_type = "info", is_directed = 0, connection_tuple_list = connection_tuple_list)
        social_undirected_graph = create_graph_for_network(network_type = "social", is_directed = 0, connection_tuple_list = connection_tuple_list)

        bio_undirected_common_node_list = find_common_node_in_graph(G = bio_undirected_graph, network_type = "bio", is_directed = 0, node_tuple_list = node_tuple_list)
        info_undirected_common_node_list = find_common_node_in_graph(G = info_undirected_graph, network_type = "info", is_directed = 0, node_tuple_list = node_tuple_list)
        social_undirected_common_node_list = find_common_node_in_graph(G = social_undirected_graph, network_type = "social", is_directed = 0, node_tuple_list = node_tuple_list)

        bio_undirected_update_sql_list = map(lambda (node1,\
                                                     node2,\
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

        info_undirected_update_sql_list = map(lambda (node1,\
                                                     node2,\
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

        social_undirected_update_sql_list = map(lambda (node1,\
                                                     node2,\
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




################################### PART3 CLASS TEST ##################################

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