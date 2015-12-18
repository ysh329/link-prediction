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
        bio_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "bio" and is_directed == 1)
        logging.info("bio_directed_tuple_rdd.count():{0}".format(bio_directed_tuple_rdd.count()))
        logging.info("bio_directed_tuple_rdd.take(3):{0}".format(bio_directed_tuple_rdd.take(3)))

        bio_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "bio" and is_directed == 0)
        logging.info("bio_undirected_tuple_rdd.count():{0}".format(bio_undirected_tuple_rdd.count()))
        logging.info("bio_undirected_tuple_rdd.take(3):{0}".format(bio_undirected_tuple_rdd.take(3)))

        # info network
        info_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "info" and is_directed == 1)
        logging.info("info_directed_tuple_rdd.count():{0}".format(info_directed_tuple_rdd.count()))
        logging.info("info_directed_tuple_rdd.take(3):{0}".format(info_directed_tuple_rdd.take(3)))

        info_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "info" and is_directed == 0)
        logging.info("info_undirected_tuple_rdd.count():{0}".format(info_undirected_tuple_rdd.count()))
        logging.info("info_undirected_tuple_rdd.take(3):{0}".format(info_undirected_tuple_rdd.take(3)))

        # social network
        social_directed_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "social" and is_directed == 1)
        logging.info("social_directed_tuple_rdd.count():{0}".format(social_directed_tuple_rdd.count()))
        logging.info("social_directed_tuple_rdd.take(3):{0}".format(social_directed_tuple_rdd.take(3)))

        social_undirected_tuple_rdd = connection_data_tuple_rdd.filter(lambda (network_type, is_directed, connection_id, node1, node2): network_type == "social" and is_directed == 0)
        logging.info("social_undirected_tuple_rdd.take(3):{0}".format(social_undirected_tuple_rdd.count()))
        logging.info("social_undirected_tuple_rdd.take(3):{0}".format(social_undirected_tuple_rdd.take(3)))

        network_rdd_list = [bio_directed_tuple_rdd, bio_undirected_tuple_rdd,\
                            info_directed_tuple_rdd, info_undirected_tuple_rdd,\
                            social_directed_tuple_rdd, social_undirected_tuple_rdd]
        return network_rdd_list



    def compute_degree_in_different_network(self, network_rdd_list):
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
                    .map(lambda (node, (in_degree, out_degree)): (node, 0 if in_degree == None else in_degree, 0 if out_degree == None else out_degree))\
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
                if network_data_tuple_rdd.take(1)[0][1] == 1: # directed
                    node_and_in_and_out_degree_tuple_rdd = compute_degree_in_directed_network(network_data_tuple_rdd)
                    node_num_in_network = node_and_in_and_out_degree_tuple_rdd.count()

                    return node_num_in_network, node_and_in_and_out_degree_tuple_rdd
                else: # undirected
                    node_and_degree_tuple_rdd = compute_degree_in_undirected_network(network_data_tuple_rdd)
                    node_num_in_network = node_and_degree_tuple_rdd.count()

                    return node_num_in_network, node_and_degree_tuple_rdd
            except Exception as e:
                logging.error("Compute node number failed in network.")
                logging.error(e)
                return None

        # sub-function
        def compute_normalized_degree_in_network(network_data_tuple_rdd):
            try:
                #print "network_data_tuple_rdd.take(1):{0}".format(network_data_tuple_rdd.take(1))
                if network_data_tuple_rdd.take(1)[0][1] == 1: # directed
                    node_num_in_network, node_and_in_and_out_degree_tuple_rdd = compute_node_num_in_network(network_data_tuple_rdd)

                    logging.info("node_num_in_network:{0}".format(node_num_in_network))
                    logging.info("node_and_in_and_out_degree_tuple_rdd.take(3):{0}".format(node_and_in_and_out_degree_tuple_rdd.take(3)))

                    node_data_tuple_rdd = node_and_in_and_out_degree_tuple_rdd\
                        .map(lambda (node, in_degree, out_degree):(node,\
                                                                   (in_degree+out_degree,\
                                                                    in_degree,\
                                                                    out_degree,\
                                                                    float(in_degree)/(in_degree+out_degree) if (in_degree+out_degree) != 0 else 0.0,\
                                                                    float(out_degree)/node_num_in_network\
                                                                    )\
                                                                   )\
                             )\
                        .sortBy(lambda (node, (degree, in_degree, out_degree, in_degree_rate, normalized_degree)): normalized_degree, False)\
                        .fullOuterJoin(compute_degree_str_in_network(network_data_tuple_rdd))\
                        .map(lambda (node,\
                                     ((degree, in_degree, out_degree, in_degree_rate, normalized_degree),\
                                      (all_degree_str, in_degree_str, out_degree_str)\
                                     )\
                                    ): (node,\
                                        all_degree_str,\
                                        degree,\
                                        in_degree_str,\
                                        in_degree,\
                                        out_degree_str,\
                                        out_degree,\
                                        in_degree_rate,\
                                        normalized_degree\
                                        )\
                             )
                    logging.info("node_data_tuple_rdd.take(3):{0}".format(node_data_tuple_rdd.take(3)))

                else: # undirected
                    node_num_in_network, node_and_degree_tuple_rdd = compute_node_num_in_network(network_data_tuple_rdd)

                    logging.info("node_num_in_network:{0}".format(node_num_in_network))
                    logging.info("node_and_degree_tuple_rdd.take(3):{0}".format(node_and_degree_tuple_rdd.take(3)))

                    node_data_tuple_rdd = node_and_degree_tuple_rdd\
                        .map(lambda (node, degree): (node, (degree, 0, 0, 0.0, float(degree) / node_num_in_network) ) )\
                        .sortBy(lambda (node, (degree, in_degree, out_degree, in_degree_rate, normalized_degree)): normalized_degree, False)\
                        .fullOuterJoin(compute_degree_str_in_network(network_data_tuple_rdd))\
                        .map(lambda (node,\
                                     ((degree, in_degree, out_degree, in_degree_rate, normalized_degree),\
                                      (all_degree_str, in_degree_str, out_degree_str)\
                                     )\
                                    ): (node,\
                                        all_degree_str,\
                                        degree,\
                                        in_degree_str,\
                                        in_degree,\
                                        out_degree_str,\
                                        out_degree,\
                                        in_degree_rate,\
                                        normalized_degree\
                                        )\
                             )
                    logging.info("node_data_tuple_rdd.take(3):{0}".format(node_data_tuple_rdd.take(3)))
            except Exception as e:
                logging.error("Compute normalized degree failed in network.")
                logging.error(e)
                return None
            return node_data_tuple_rdd

        # sub-function
        def compute_degree_str_in_network(network_data_tuple_rdd):
            try:
                # WHATEVER directed or !directed
                node1_and_node2_tuple_rdd = network_data_tuple_rdd\
                    .map(lambda (network_type, is_directed, connection_id, node1, node2): (node1, node2))
                node_and_in_degree_str_tuple_rdd = node1_and_node2_tuple_rdd\
                    .map(lambda (node1, node2): (node2, node1))\
                    .reduceByKey(lambda in_node1, in_node2: str(in_node1)+"///"+str(in_node2))\
                    .map(lambda (node, in_degree_str): (node, str(in_degree_str)))
                #logging.info("node_and_in_degree_str_tuple_rdd.take(3):{0}".format(node_and_in_degree_str_tuple_rdd.take(3)))

                node_and_out_degree_str_tuple_rdd = node1_and_node2_tuple_rdd\
                    .map(lambda (node1, node2): (node1, node2))\
                    .reduceByKey(lambda out_node1, out_node2: str(out_node1)+"///"+str(out_node2))\
                    .map(lambda (node, in_degree_str): (node, str(in_degree_str)))
                #logging.info("node_and_out_degree_str_tuple_rdd.take(3):{0}".format(node_and_out_degree_str_tuple_rdd.take(3)))

                node_and_in_and_out_degree_str_tuple_rdd = node_and_in_degree_str_tuple_rdd\
                    .fullOuterJoin(node_and_out_degree_str_tuple_rdd)\
                    .map(lambda (node, (in_degree_str, out_degree_str)): (node,\
                                                                          "" if in_degree_str == None else in_degree_str,\
                                                                          "" if out_degree_str == None else out_degree_str\
                                                                          )\
                         )

                node_and_all_and_in_and_out_degree_str_tuple_rdd = node_and_in_and_out_degree_str_tuple_rdd\
                    .map(lambda (node, in_degree_str, out_degree_str): (node,\
                                                                        ("///".join([i for i in set(in_degree_str.split("///")+out_degree_str.split("///")) if i!=""]),\
                                                                         in_degree_str,\
                                                                         out_degree_str\
                                                                         )\
                                                                        )\
                         )
            except Exception as e:
                logging.error("failed to compute degree string in network.")
                logging.error(e)
                return None
            return node_and_all_and_in_and_out_degree_str_tuple_rdd

        bio_directed_tuple_rdd, bio_undirected_tuple_rdd = network_rdd_list[0], network_rdd_list[1]
        info_directed_tuple_rdd, info_undirected_tuple_rdd = network_rdd_list[2], network_rdd_list[3]
        social_directed_tuple_rdd, social_undirected_tuple_rdd = network_rdd_list[4], network_rdd_list[5]

        # (node, degree, in_degree, out_degree, in_degree_rate, normalized_degree)
        bio_directed_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = bio_directed_tuple_rdd)
        bio_undirected_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = bio_undirected_tuple_rdd)

        info_directed_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = info_directed_tuple_rdd)
        info_undirected_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = info_undirected_tuple_rdd)

        social_directed_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = social_directed_tuple_rdd)
        social_undirected_node_data_tuple_rdd = compute_normalized_degree_in_network(network_data_tuple_rdd = social_undirected_tuple_rdd)

        # Merge them into one list
        node_data_rdd_list = [bio_directed_node_data_tuple_rdd, bio_undirected_node_data_tuple_rdd,\
                              info_directed_node_data_tuple_rdd, info_undirected_node_data_tuple_rdd,\
                              social_directed_node_data_tuple_rdd, social_undirected_node_data_tuple_rdd]
        return node_data_rdd_list



    def save_node_data_rdd_list_to_database(self, database_name, node_table_name, node_data_rdd_list):
        # sub-function
        # (node, all_degree_str, degree_num, in_degree_str, in_degree_num,
        #  out_degree_str, out_degree_num, in_degree_rate, normalized_degree)
        def sql_generator(database_name, node_table_name, network_type, is_directed, node_data_tuple):
            node = node_data_tuple[0]
            degree_str = node_data_tuple[1]
            degree_num = node_data_tuple[2]
            in_degree_str = node_data_tuple[3]
            in_degree_num = node_data_tuple[4]
            out_degree_str = node_data_tuple[5]
            out_degree_num = node_data_tuple[6]
            in_degree_rate = float(out_degree_num)/(in_degree_num+out_degree_num) if out_degree_num != 0 else 0#node_data_tuple[7]
            normalized_degree = node_data_tuple[8]
            try:
                sql = """INSERT INTO {database_name}.{table_name}(node, network_type, is_directed, degree_str, degree_num,
                                                                  in_degree_str, in_degree_num,
                                                                  out_degree_str, out_degree_num,
                                                                  in_degree_rate, normalized_degree)
                         VALUES({node}, '{network_type}', {is_directed}, '{degree_str}', {degree_num},
                                '{in_degree_str}', {in_degree_num},
                                '{out_degree_str}', {out_degree_num},
                                {in_degree_rate}, {normalized_degree})"""\
                    .format(database_name = database_name,\
                            table_name = node_table_name,\
                            node = node, network_type = network_type, is_directed = is_directed,\
                            degree_str = degree_str, degree_num = degree_num,\
                            in_degree_str = in_degree_str, in_degree_num = out_degree_num,\
                            out_degree_str = out_degree_str, out_degree_num = in_degree_num,\
                            in_degree_rate = in_degree_rate, normalized_degree = normalized_degree\
                            )
            except Exception as e:
                logging.error("failed to generate sql.")
                logging.error(e)
            return sql


        bio_directed_node_data_tuple_rdd, bio_undirected_node_data_tuple_rdd = node_data_rdd_list[0], node_data_rdd_list[1]
        info_directed_node_data_tuple_rdd, info_undirected_node_data_tuple_rdd = node_data_rdd_list[2], node_data_rdd_list[3]
        social_directed_node_data_tuple_rdd, social_undirected_node_data_tuple_rdd = node_data_rdd_list[4], node_data_rdd_list[5]


        # generate sql from rdd
        # bio
        bio_directed_node_sql_rdd = bio_directed_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "bio",\
                                                       is_directed = 1,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("bio_directed_node_sql_rdd.persist().is_cached:{0}".format(bio_directed_node_sql_rdd.persist().is_cached))
        logging.info("bio_directed_node_sql_rdd.count():{0}".format(bio_directed_node_sql_rdd.count()))
        logging.info("bio_directed_node_sql_rdd.take(3):{0}".format(bio_directed_node_sql_rdd.take(3)))
        bio_undirected_node_sql_rdd = bio_undirected_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "bio",\
                                                       is_directed = 0,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("bio_undirected_node_sql_rdd.persist().is_cached:{0}".format(bio_undirected_node_sql_rdd.persist().is_cached))
        logging.info("bio_undirected_node_sql_rdd.count():{0}".format(bio_undirected_node_sql_rdd.count()))
        logging.info("bio_undirected_node_sql_rdd.take(3):{0}".format(bio_undirected_node_sql_rdd.take(3)))

        # info
        info_directed_node_sql_rdd = info_directed_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "info",\
                                                       is_directed = 1,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("info_directed_node_sql_rdd.persist().is_cached:{0}".format(info_directed_node_sql_rdd.persist().is_cached))
        logging.info("info_directed_node_sql_rdd.count():{0}".format(info_directed_node_sql_rdd.count()))
        logging.info("info_directed_node_sql_rdd.take(3):{0}".format(info_directed_node_sql_rdd.take(3)))
        info_undirected_node_sql_rdd = info_undirected_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "info",\
                                                       is_directed = 0,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("info_undirected_node_sql_rdd.persist().is_cached:{0}".format(info_undirected_node_sql_rdd.persist().is_cached))
        logging.info("info_undirected_node_sql_rdd.count():{0}".format(info_undirected_node_sql_rdd.count()))
        logging.info("info_undirected_node_sql_rdd.take(3):{0}".format(info_undirected_node_sql_rdd.take(3)))

        # social
        social_directed_node_sql_rdd = social_directed_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "social",\
                                                       is_directed = 1,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("social_directed_node_sql_rdd.persist().is_cached:{0}".format(social_directed_node_sql_rdd.persist().is_cached))
        logging.info("social_directed_node_sql_rdd.count():{0}".format(social_directed_node_sql_rdd.count()))
        logging.info("social_directed_node_sql_rdd.take(3):{0}".format(social_directed_node_sql_rdd.take(3)))

        social_undirected_node_sql_rdd = social_undirected_node_data_tuple_rdd\
            .map(lambda node_data_tuple: sql_generator(database_name = database_name,\
                                                       node_table_name = node_table_name,\
                                                       network_type = "social",\
                                                       is_directed = 0,\
                                                       node_data_tuple = node_data_tuple\
                                                       )\
                 )
        logging.info("social_undirected_node_sql_rdd.persist().is_cached:{0}".format(social_undirected_node_sql_rdd.persist().is_cached))
        logging.info("social_undirected_node_sql_rdd.count():{0}".format(social_undirected_node_sql_rdd.count()))
        logging.info("social_undirected_node_sql_rdd.take(3):{0}".format(social_undirected_node_sql_rdd.take(3)))

        # prepare to insert sql to database
        node_sql_rdd_list = [bio_directed_node_sql_rdd, bio_undirected_node_sql_rdd,\
                             info_directed_node_sql_rdd, info_undirected_node_sql_rdd,\
                             social_directed_node_sql_rdd, social_undirected_node_sql_rdd]

        cursor = self.con.cursor()
        success_update = 0
        failure_update = 0

        split_num_for_each_rdd = 32
        for rdd_idx in xrange(len(node_sql_rdd_list)):
            node_sql_rdd = node_sql_rdd_list[rdd_idx]
            logging.info("==========={0}th rdd_idx===========".format(rdd_idx))
            sub_node_sql_rdd_list = node_sql_rdd.randomSplit(xrange(split_num_for_each_rdd))
            for sub_rdd_idx in xrange(len(sub_node_sql_rdd_list)):
                sub_rdd = sub_node_sql_rdd_list[sub_rdd_idx]
                sub_node_sql_list = sub_rdd.collect()
                logging.info("==========={0}th sub_rdd_idx===========".format(sub_rdd_idx))
                for sql_idx in xrange(len(sub_node_sql_list)):
                    sql = sub_node_sql_list[sql_idx]
                    if (sql_idx % 10000 == 0 and sql_idx > 9998) or (sql_idx == len(sub_node_sql_list) -1):
                        logging.info("==========={0}th element in sub_node_sql_list===========".format(sql_idx))
                        logging.info("sql_execute_index:{idx}, finish rate:{rate}".format(idx = sql_idx, rate = float(sql_idx+1)/ len(sub_node_sql_list)))
                        logging.info("success_rate:{success_rate}".format(success_rate = success_update/ float(success_update + failure_update + 1)))
                        logging.info("success_update:{success}, failure_update:{failure}".format(success = success_update, failure = failure_update))
                    try:
                        cursor.execute(sql)
                        self.con.commit()
                        success_update = success_update + 1
                    except MySQLdb.Error, e:
                        self.con.rollback()
                        logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                        logging.error("error SQL:{0}".format(sql))
                        failure_update = failure_update + 1
        cursor.close()

        # un-persist rdd previously persisted rdd
        logging.info("bio_directed_node_sql_rdd.unpersist().is_cached:{0}".format(bio_directed_node_sql_rdd.unpersist().is_cached))
        logging.info("bio_undirected_node_sql_rdd.unpersist().is_cached:{0}".format(bio_undirected_node_sql_rdd.unpersist().is_cached))
        logging.info("info_directed_node_sql_rdd.persist().is_cached:{0}".format(info_directed_node_sql_rdd.unpersist().is_cached))
        logging.info("info_undirected_node_sql_rdd.persist().is_cached:{0}".format(info_undirected_node_sql_rdd.unpersist().is_cached))
        logging.info("social_directed_node_sql_rdd.persist().is_cached:{0}".format(social_directed_node_sql_rdd.unpersist().is_cached))
        logging.info("social_undirected_node_sql_rdd.persist().is_cached:{0}".format(social_undirected_node_sql_rdd.unpersist().is_cached))


################################### PART3 CLASS TEST ##################################
'''
# Initialization
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
node_table_name = "node_table"
from pyspark import SparkContext
pyspark_sc = SparkContext()

Computer = ComputeNodeProperty(database_name = database_name, pyspark_sc = pyspark_sc)
network_rdd_list = Computer.read_connection_data_in_database(database_name = database_name,\
                                                             connection_table_name = connection_table_name)
node_data_rdd_list = Computer.compute_degree_in_different_network(network_rdd_list = network_rdd_list)
Computer.save_node_data_rdd_list_to_database(database_name = database_name,\
                                             node_table_name = node_table_name,\
                                             node_data_rdd_list = node_data_rdd_list)
'''