import os
from pyspark import SparkContext, SparkConf, sql
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import dense_rank
from pyspark.sql.window import Window
from functools import reduce

COUNTRIES = ['US', 'MX', 'CA']
SCHEMA = "MotelID STRING, BidDate STRING, HU STRING, UK DOUBLE, NL DOUBLE, US DOUBLE, MX DOUBLE, AU DOUBLE, CA " \
         "DOUBLE, CN DOUBLE, KR DOUBLE, BE DOUBLE, I DOUBLE, JP DOUBLE, IN DOUBLE, HN DOUBLE, GY DOUBLE, DE DOUBLE"


os.environ['JAVA_HOME'] = 'C:\\Progra~1\\Java\\jdk1.8.0_181'
os.environ['HADOOP_HOME'] = 'C:\\hadoop'


def get_errors_df(df):
    """This function returns a DataFrame with all the erroneous records from df grouped by date and code with counts.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :return: DataFrame.
    """
    return df.filter(df.HU.contains('ERROR')).select('BidDate', 'HU').groupBy('BidDate', 'HU').count()


def get_clear_df(df, sql_context):
    """This function returns the clear (errorless) df with rows divided by countries.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :param sql_context: SQLContext to use.
    :type sql_context: SQLContext.
    :return: DataFrame.
    """
    clear_df = df.filter(~df.HU.contains('ERROR'))
    country_dfs = [clear_df.selectExpr('MotelId', 'BidDate', '"{}"'.format(country), country) for country in COUNTRIES]
    zero_df = sql_context.createDataFrame([], schema='MotelId STRING, BidDate STRING, country STRING, price DOUBLE')
    return reduce(sql.DataFrame.union, country_dfs, zero_df).dropna()


def get_eur_bids_df(df, path_to_exchange, sql_context):
    """This function simply converts the bid prices from dollar to euro.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :param sql_context: SQLContext to use.
    :type sql_context: SQLContext.
    :param path_to_exchange: Path to the exchange rates dataset.
    :type path_to_exchange: str.
    :return: DataFrame.
    """
    exchange_rates = broadcast(sql_context.read
                               .csv(path_to_exchange,
                                    schema='date STRING, curr_long STRING, curr_short STRING, factor DOUBLE'))
    df_joined = df.join(exchange_rates, df.BidDate == exchange_rates.date)
    return df_joined.selectExpr('MotelId', 'BidDate', 'country', 'price * factor AS price')


def get_motels_names_df(df, path_to_motels, sql_context):
    """This function enriches the bid DataFrame with the names of the motels.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :param path_to_motels: Path to the motels dataset.
    :type path_to_motels: str.
    :param sql_context: SQLContext to use.
    :type sql_context: SQLContext.
    :return: DataFrame.
    """
    motels = broadcast(sql_context.read.csv(path_to_motels, schema='id STRING, name STRING, motel_country STRING, url STRING, comment DOUBLE'))
    df_joined = df.join(motels, df.MotelId == motels.id)
    return df_joined.select('MotelId', 'name', 'BidDate', 'country', 'price')


def get_max_bids_df(df):
    """This functions returns only the rows with the maximum (all of them) price per date and motel ID.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :return: DataFrame.
    """
    window = Window.partitionBy('BidDate', 'MotelId').orderBy(df.price.desc())
    df_ranked = df.withColumn('rank', dense_rank().over(window))
    return df_ranked.filter(df_ranked.rank == 1).select('MotelId', 'name', 'BidDate', 'country', 'price')


conf = SparkConf().setMaster('local').setAppName('test')
sc = SparkContext(conf=conf)
sql_sc = sql.HiveContext(sc)

raw_unclear_bids = sql_sc.read.csv('..\\input\\bids_short.txt', schema=SCHEMA)
error_bids = get_errors_df(raw_unclear_bids)

bids = get_clear_df(raw_unclear_bids, sql_sc)
bids = get_eur_bids_df(bids, '..\\input\\exchange_rate.txt', sql_sc)
bids = get_motels_names_df(bids, '..\\input\\motels.txt', sql_sc)
bids = get_max_bids_df(bids)

bids.write.csv('..\\output\\bids')
