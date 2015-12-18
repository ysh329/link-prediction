# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_data_collation_for_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-05 20:52:10
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import MySQLdb
import logging
import time

################################### PART2 CLASS && FUNCTION ###########################
class DataCollationInDB(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = DataCollationInDB.__name__))

        try:
            self.con = MySQLdb.connect(host='localhost', user='root', passwd='931209', charset='utf8')
            logging.info("Success in connecting MySQL.")
        except MySQLdb.Error, e:
            logging.error("Fail in connecting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))



    def __del__(self):
        try:
            self.con.close()
            logging.info("Success in quiting MySQL.")
        except MySQLdb.Error, e:
            self.con.rollback()
            logging.error("Fail in quiting MySQL.")
            logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        logging.info("END CLASS {class_name}.".format(class_name = DataCollationInDB.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = DataCollationInDB.__name__, delta_time = self.end - self.start))



    def get_data_from_database(self, database_name, node_table_name, connection_table_name):
        cursor = self.con.cursor()
        sql_list = ["SELECT network_type, is_directed,"
                    " node, degree_str, degree_num, in_degree_str, in_degree_num, out_degree_str, out_degree_num"
                    " FROM {database_name}.{table_name}".format(database_name = database_name, table_name = node_table_name)]
        sql_list.append("SELECT network_type, is_directed, node1, node2"
                        " FROM {database_name}.{table_name}".format(database_name = database_name, table_name = connection_table_name))
        for sql_idx in xrange(len(sql_list)):
            sql = sql_list[sql_idx]
            try:
                cursor.execute(sql)
            except MySQLdb.Error as e:
                logging.error("Fail in getting MySQL data.")
                logging.error("error sql:{0}".format(sql))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))

            if sql_idx == 0:
                node_tuple_list = map(lambda (network_type,\
                                                 is_directed,\
                                                 node,\
                                                 degree_str,\
                                                 degree_num,\
                                                 in_degree_str,\
                                                 in_degree_num,\
                                                 out_degree_str,\
                                                 out_degree_num):(str(network_type),\
                                                                  int(is_directed),\
                                                                  int(node),\
                                                                  str(degree_str),\
                                                                  int(degree_num),\
                                                                  str(in_degree_str),\
                                                                  int(in_degree_num),\
                                                                  str(out_degree_str),\
                                                                  int(out_degree_num)),\
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
                                                                int(node2)),\
                                            cursor.fetchall()\
                                            )
                logging.info("len(connection_tuple_list):{0}".format(len(connection_tuple_list)))
                logging.info("connection_tuple_list[:3]:{0}".format(connection_tuple_list[:3]))
        return node_tuple_list, connection_tuple_list

    def sql_generator_for_all_connection(self, database_name, connection_table_name, node_tuple_list, connection_tuple_list):
        def generate_sql_for_network(database_name, connection_table_name, network_type, is_directed, node_tuple_list, connection_tuple_list):
            node_tuple_list = filter(lambda (_network_type,\
                                                _is_directed,\
                                                node,\
                                                degree_str,\
                                                degree_num,\
                                                in_degree_str,\
                                                in_degree_num,\
                                                out_degree_str,\
                                                out_degree_num): _network_type == network_type and _is_directed == is_directed,\
                                     node_tuple_list\
                                     )
            node_tuple_list = map(lambda (network_type,\
                                             is_directed,\
                                             node,\
                                             degree_str,\
                                             degree_num,\
                                             in_degree_str,\
                                             in_degree_num,\
                                             out_degree_str,\
                                             out_degree_num): (node,\
                                                               degree_str,\
                                                               degree_num,\
                                                               in_degree_str,\
                                                               in_degree_num,\
                                                               out_degree_str,\
                                                               out_degree_num),\
                                  node_tuple_list\
                                  )
            logging.info("len(node_tuple_list):{0}".format(len(node_tuple_list)))
            logging.info("node_tuple_list[:3]:{0}".format(node_tuple_list[:3]))

            connection_tuple_list = filter(lambda (_network_type,\
                                                      _is_directed,\
                                                      node1,\
                                                      node2): _network_type == network_type and _is_directed == is_directed,\
                                           connection_tuple_list\
                                           )
            connection_tuple_list = map(lambda (network_type,\
                                                   is_directed,\
                                                   node1,\
                                                   node2): (node1,\
                                                            node2),\
                                        connection_tuple_list\
                                        )
            logging.info("len(connection_tuple_list):{0}".format(len(connection_tuple_list)))
            logging.info("connection_tuple_list[:3]:{0}".format(connection_tuple_list[:3]))

            success_generate = 0
            failure_generate = 0
            sql_list = []

            for connection_idx in xrange(len(connection_tuple_list)):
                connection_tuple = connection_tuple_list[connection_idx]
                node1 = connection_tuple[0]
                node2 = connection_tuple[1]
                try:
                    node1_tuple_list = filter(lambda node_tuple: node_tuple[0] == node1, node_tuple_list)[0]
                    node2_tuple_list = filter(lambda node_tuple: node_tuple[0] == node2, node_tuple_list)[0]

                    node1_degree_str = node1_tuple_list[1]
                    node2_degree_str = node2_tuple_list[1]

                    node1_degree_num = node1_tuple_list[2]
                    node2_degree_num = node2_tuple_list[2]

                    node1_in_degree_str = node1_tuple_list[3]
                    node2_in_degree_str = node2_tuple_list[3]

                    node1_in_degree_num = node1_tuple_list[4]
                    node2_in_degree_num = node2_tuple_list[4]

                    node1_out_degree_str = node1_tuple_list[5]
                    node2_out_degree_str = node2_tuple_list[5]

                    node1_out_degree_num = node1_tuple_list[6]
                    node2_out_degree_num = node2_tuple_list[6]

                    sql_list.append("UPDATE {database_name}.{table_name}"
                                    " SET node1_degree_str = '{node1_degree_str}',"
                                    "    node2_degree_str = '{node2_degree_str}',"
                                    "    node1_degree_num = {node1_degree_num},"
                                    "    node2_degree_num = {node2_degree_num},"
                                    "    node1_in_degree_str = '{node1_in_degree_str}',"
                                    "    node2_in_degree_str = '{node2_in_degree_str}',"
                                    "    node1_in_degree_num = {node1_in_degree_num},"
                                    "    node2_in_degree_num = {node2_in_degree_num},"
                                    "    node1_out_degree_str = '{node1_out_degree_str}',"
                                    "    node2_out_degree_str = '{node2_out_degree_str}',"
                                    "    node1_out_degree_num = {node1_out_degree_num},"
                                    "    node2_out_degree_num = {node2_out_degree_num}"
                                    " WHERE network_type = '{network_type}' AND is_directed = {is_directed}"\
                                    .format(database_name = database_name,\
                                            table_name = connection_table_name,\
                                            node1_degree_str = node1_degree_str, node2_degree_str = node2_degree_str,\
                                            node1_degree_num = node1_degree_num, node2_degree_num = node2_degree_num,\
                                            node1_in_degree_str = node1_in_degree_str, node2_in_degree_str = node2_in_degree_str,\
                                            node1_in_degree_num = node1_in_degree_num, node2_in_degree_num = node2_in_degree_num,\
                                            node1_out_degree_str = node1_out_degree_str, node2_out_degree_str = node2_out_degree_str,\
                                            node1_out_degree_num = node1_out_degree_num, node2_out_degree_num = node2_out_degree_num,\
                                            network_type = network_type, is_directed = is_directed),\
                                    )
                    success_generate = success_generate + 1

                except Exception as e:
                    logging.error(e)
                    logging.error("network:{network_type}.{is_directed} connection_idx:{connection_idx}"\
                                  .format(network_type = network_type,\
                                          is_directed = is_directed,\
                                          connection_idx = connection_idx)\
                                  )
                    failure_generate = failure_generate + 1
            logging.info("len(sql_list):{0}".format(len(sql_list)))
            logging.info("sql_list[:3]:{0}".format(sql_list[:3]))
            return  sql_list

        def execute_sql_in_database(self, network_type, is_directed, sql_list):
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
                    logging.error("Fail in update {network_type}.{is_directed} sql_idx:{sql_idx}".format(network_type = network_type, is_directed = is_directed, sql_idx =sql_idx))
                    logging.error("error sql:{0}".format(sql))
                    logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
                    failure_update = failure_update + 1


        bio_1_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'bio', is_directed = 1, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'bio', is_directed = 1, sql_list = bio_1_sql_list)

        bio_0_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'bio', is_directed = 0, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'bio', is_directed = 0, sql_list = bio_0_sql_list)

        info_1_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'info', is_directed = 1, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'info', is_directed = 1, sql_list = info_1_sql_list)

        info_0_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'info', is_directed = 0, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'info', is_directed = 0, sql_list = info_0_sql_list)

        social_1_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'social', is_directed = 1, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'social', is_directed = 1, sql_list = social_1_sql_list)

        social_0_sql_list = generate_sql_for_network(database_name = database_name, connection_table_name = connection_table_name, network_type = 'social', is_directed = 0, node_tuple_list = node_tuple_list, connection_tuple_list = connection_tuple_list)
        execute_sql_in_database(self = self, network_type = 'social', is_directed = 0, sql_list = social_0_sql_list)



################################### PART3 CLASS TEST ##################################
''''
# initial parameters
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
node_table_name = "node_table"

DC = DataCollationInDB()
node_tuple_list, connection_tuple_list = DC\
    .get_data_from_database(database_name = database_name,\
                            node_table_name = node_table_name,\
                            connection_table_name = connection_table_name)
sql_list = DC\
    .sql_generator_for_all_connection(node_tuple_list = node_tuple_list,\
                                      connection_tuple_list = connection_tuple_list)
'''