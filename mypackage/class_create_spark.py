# -*- coding: utf-8 -*-
# !/usr/bin/python
################################### PART0 DESCRIPTION #################################
# Filename: class_save_word_to_database.py
# Description:
#


# Author: Shuai Yuan
# E-mail: ysh329@sina.com
# Create: 2015-12-06 15:28:05
# Last:
__author__ = 'yuens'

################################### PART1 IMPORT ######################################
import logging
import time
from pyspark import SparkContext, SparkConf
################################### PART2 CLASS && FUNCTION ###########################
class CreateSpark(object):
    def __init__(self, pyspark_app_name):
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
        logging.info("START CLASS {class_name}.".format(class_name = CreateSpark.__name__))

        # Configure Spark
        try:
            conf = SparkConf().setAppName(pyspark_app_name).setMaster("local[8]")
            self.sc = SparkContext(conf = conf)
            logging.info("Start pyspark successfully.")
            logging.info("sc.version:{0}".format(self.sc.version))
        except Exception as e:
            logging.error("Fail in starting pyspark.")
            logging.error(e)



    def return_spark_context(self):
        return self.sc



    def __del__(self):
        # Close SparkContext
        try:
            self.sc.stop()
            logging.info("close SparkContext successfully.")
        except Exception as e:
            logging.error(e)

        logging.info("END CLASS {class_name}.".format(class_name = CreateSpark.__name__))
        self.end = time.clock()
        logging.info("The class {class_name} run time is : {delta_time} seconds".format(class_name = CreateSpark.__name__, delta_time = self.end))


################################### PART3 CLASS TEST ##################################
"""
# Initialization
pyspark_app_name = "link-prediction"

SparkCreator = CreateSpark(pyspark_app_name = pyspark_app_name)
pyspark_sc = SparkCreator.return_spark_context()
"""
