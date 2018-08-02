"""
This module contains the tests for the function get_clear_df from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from bids import get_eur_bids_df


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = StructType([StructField('MotelId', StringType(), True),
                                 StructField('BidDate', StringType(), True),
                                 StructField('country', StringType(), True),
                                 StructField('price', DoubleType(), True)])
        cls.conf = SparkConf().setMaster('local').setAppName('testing')
        cls.sc = SparkContext(conf=cls.conf)
        cls.sql_sc = SQLContext(cls.sc)
        cls.first_test_df = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_eur_bids_df\\first.txt',
                                                schema=cls.schema)
        cls.correct_first = cls.sql_sc.read.csv('inputs\\inputs_for_function_get_eur_bids_df\\correct_first.txt',
                                                schema=cls.schema)
        cls.path_to_exchange = 'inputs\\inputs_for_function_get_eur_bids_df\\exchange_rate.txt'

    def test_first(self):
        result_df = get_eur_bids_df(self.first_test_df, self.path_to_exchange, self.sql_sc)
        self.assertEqual(set(result_df.collect()), set(self.correct_first.collect()))


if __name__ == '__main__':
    unittest.main()
