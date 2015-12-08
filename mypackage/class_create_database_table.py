# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_create_database_table.py
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
class CreateDatabaseTable(object):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateDatabaseTable.__name__))

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
        logging.info("END CLASS {class_name}.".format(class_name = CreateDatabaseTable.__name__))

        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateDatabaseTable.__name__, delta_time = self.end - self.start))



    def create_database(self, database_name):
        logging.info("database name: {database_name}".format(database_name = database_name))

        cursor = self.con.cursor()
        sqls = ['SET NAMES UTF8', 'SELECT VERSION()', "CREATE DATABASE {database_name}".format(database_name = database_name)]

        for sql_idx in xrange(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                if sql_idx == 1:
                    result = cursor.fetchall()[0]
                    mysql_version = result[0]
                    logging.info("MySQL VERSION: {mysql_version}".format(mysql_version = mysql_version))
                self.con.commit()
                logging.info("Success in creating database {database_name}.".format(database_name = database_name))
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating database {database_name}.".format(database_name = database_name))
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()



    def create_table(self, database_name, connection_table_name, node_table_name):
        cursor = self.con.cursor()
        sqls = ["USE {database_name}".format(database_name = database_name), 'SET NAMES UTF8']

        sqls.append("ALTER DATABASE {database_name} DEFAULT CHARACTER SET 'utf8'".format(database_name = database_name))

        # Create node_table_name
        sqls.append("""CREATE TABLE IF NOT EXISTS {node_table_name}(
                                node_record_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                network_type VARCHAR(20),
                                is_directed INT(11),
                                node_id INT(11),
                                degree TEXT,
                                degree_num INT(11),
                                in_degree TEXT,
                                in_degree_num INT(11),
                                out_degree TEXT,
                                out_degree_num INT(11),
                                normalized_degree FLOAT,
                                closenesss_centrality FLOAT,
                                between_centrality FLOAT,
                                eigen_vector_centrality FLOAT,
                                UNIQUE (node_record_id))""".format(node_table_name = node_table_name))
        sqls.append("""CREATE INDEX node_id_idx ON {node_table_name}(node_id)""".format(node_table_name = node_table_name))

        # Create connection_table_name
        sqls.append("""CREATE TABLE IF NOT EXISTS {connection_table_name}(
                                connection_record_id INT(11) AUTO_INCREMENT PRIMARY KEY,
                                network_type VARCHAR(20),
                                is_directed INT(11),
                                connection_id INT(11),
                                node1 INT(11),
                                node2 INT(11),
                                UNIQUE (connection_record_id))""".format(connection_table_name = connection_table_name))
        sqls.append("""CREATE INDEX connection_id_idx ON {connection_table_name}(connection_id)""".format(connection_table_name = connection_table_name))

        for sql_idx in range(len(sqls)):
            sql = sqls[sql_idx]
            try:
                cursor.execute(sql)
                self.con.commit()
                logging.info("Success in creating table.")
            except MySQLdb.Error, e:
                self.con.rollback()
                logging.error("Fail in creating table.")
                logging.error("MySQL Error {error_num}: {error_info}.".format(error_num = e.args[0], error_info = e.args[1]))
        cursor.close()



################################### PART3 CLASS TEST ##################################
"""
# initial parameters
database_name = "LinkPredictionDB"
connection_table_name = "connection_table"
node_table_name = "node_table"

Creater = CreateDatabaseTable()
Creater.create_database(database_name = database_name)
Creater.create_table(database_name = database_name,\
                     connection_table_name = connection_table_name,\
                     node_table_name = node_table_name)
"""