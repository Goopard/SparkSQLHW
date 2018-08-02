"""
This module contains the tests for the function get_clear_df from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from bids import get_clear_df


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = StructType([StructField('MotelId', StringType(), True),
                                 StructField('BidDate', StringType(), True),
                                 StructField('HU', StringType(), True),
                                 StructField('UK', DoubleType(), True),
                                 StructField('NL', DoubleType(), True),
                                 StructField('US', DoubleType(), True),
                                 StructField('MX', DoubleType(), True),
                                 StructField('AU', DoubleType(), True),
                                 StructField('CA', DoubleType(), True),
                                 StructField('CN', DoubleType(), True),
                                 StructField('KR', DoubleType(), True),
                                 StructField('BE', DoubleType(), True),
                                 StructField('I', DoubleType(), True),
                                 StructField('JP', DoubleType(), True),
                                 StructField('IN', DoubleType(), True),
                                 StructField('HN', DoubleType(), True),
                                 StructField('GY', DoubleType(), True),
                                 StructField('GE', DoubleType(), True)])
        cls.correct_schema = StructType([StructField('MotelId', StringType(), True),
                                         StructField('BidDate', StringType(), True),
                                         StructField('country', StringType(), True),
                                         StructField('price', DoubleType(), True)])
        cls.conf = SparkConf().setMaster('local').setAppName('testing')
        cls.sc = SparkContext(conf=cls.conf)
        cls.sql_sc = SQLContext(cls.sc)
        cls.first_test_df = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_clear_df\\first.txt',
                                                schema=cls.schema)
        cls.second_test_df = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_clear_df\\second.txt',
                                                 schema=cls.schema)
        cls.correct_first = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_clear_df\\correct_first.txt',
                                                schema=cls.correct_schema)
        cls.correct_second = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_clear_df\\correct_second.txt',
                                                 schema=cls.correct_schema)

    def test_first(self):
        result_df = get_clear_df(self.first_test_df, self.sql_sc)
        self.assertEqual(set(result_df.collect()), set(self.correct_first.collect()))

    def test_second(self):
        result_df = get_clear_df(self.second_test_df, self.sql_sc)
        self.assertEqual(set(result_df.collect()), set(self.correct_second.collect()))


if __name__ == '__main__':
    unittest.main()
