"""
This module contains the tests for the function get_errors_df from the module bids.
"""
import unittest
from pyspark import SparkContext, SparkConf, SQLContext, sql
from pyspark.sql.types import StructType, StructField, StringType
from bids import get_errors_df


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = StructType([StructField('MotelId', StringType(), True),
                                 StructField('BidDate', StringType(), True),
                                 StructField('HU', StringType(), True)])
        cls.conf = SparkConf().setMaster('local').setAppName('testing')
        cls.sc = SparkContext(conf=cls.conf)
        cls.sql_sc = SQLContext(cls.sc)
        cls.first_test_df = cls.sql_sc.createDataFrame([('0000002', '14-29-04-2016', 'ERROR_BID_SERVICE_UNAVAILABLE'),
                                                        ('0000002', '14-29-04-2016', 'ERROR_BID_SERVICE_UNAVAILABLE'),
                                                        ('0000002', '14-29-04-2016', 'ERROR_BID_SERVICE_UNAVAILABLE')],
                                                       schema=cls.schema)
        cls.second_test_df = cls.sql_sc.createDataFrame([('0000002', '14-29-04-2016', 'ERROR_BID_SERVICE_UNAVAILABLE'),
                                                         ('0000002', '14-29-04-2016', 'ERROR_BID_SERVICE_UNAVAILABLE'),
                                                         ('0000005', '04-08-02-2016', 'ERROR_ACCESS_DENIED'),
                                                         ('0000005', '04-08-02-2016', 'ERROR_ACCESS_DENIED')],
                                                        schema=cls.schema)

    def test_3_same_errors(self):
        result_df = get_errors_df(self.first_test_df)
        self.assertEqual(list(map(sql.Row.asDict, result_df.collect())),
                         [{'BidDate': '14-29-04-2016', 'HU': 'ERROR_BID_SERVICE_UNAVAILABLE', 'count': 3}])

    def test_some_different_errors(self):
        result_df = get_errors_df(self.second_test_df)
        self.assertEqual(list(map(sql.Row.asDict, result_df.collect())),
                         [{'BidDate': '04-08-02-2016', 'HU': 'ERROR_ACCESS_DENIED', 'count': 2},
                          {'BidDate': '14-29-04-2016', 'HU': 'ERROR_BID_SERVICE_UNAVAILABLE', 'count': 2}])


if __name__ == '__main__':
    unittest.main()
