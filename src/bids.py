from pyspark import SparkContext, SparkConf, sql
from pyspark.sql.functions import broadcast, dense_rank, udf, explode, array, isnull, lit, col, expr, unix_timestamp, \
    from_unixtime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, ArrayType


COUNTRIES = ['US', 'MX', 'CA']
SCHEMA = StructType([StructField('MotelId', StringType(), True),
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
INPUT_DIR = '/user/raj_ops/bid_data_large/'
OUTPUT_DIR = '/user/raj_ops/'


def get_errors_df(df):
    """This function returns a DataFrame with all the erroneous records from df grouped by date and code with counts.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :return: DataFrame.
    """
    return df.filter(df.HU.contains('ERROR')).select('BidDate', 'HU').groupBy('BidDate', 'HU').count()


def get_clear_df(df):
    """This function returns the clear (errorless) df with rows divided by countries.

    :param df: Input DataFrame.
    :type df: DataFrame.
    :return: DataFrame.
    """
    combine = udf(lambda x, y: list(zip(x, y)), ArrayType(StructType([StructField("country", StringType()),
                                                                      StructField("price", StringType())])))
    clear_df = df.filter(~df.HU.contains('ERROR') | isnull(df.HU))
    return clear_df.withColumn('temp', combine(array(list(map(lit, COUNTRIES))), array(COUNTRIES)))\
        .withColumn('temp', explode('temp'))\
        .select('MotelId',
                from_unixtime(unix_timestamp('BidDate', 'HH-dd-MM-yyyy')).alias('BidDate'),
                col('temp.country').alias('country'),
                col('temp.price').alias('price')).dropna()


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
    schema = StructType([StructField('date', StringType(), True),
                         StructField('curr_long', StringType(), True),
                         StructField('curr_short', StringType(), True),
                         StructField('factor', DoubleType(), True)])
    exchange_rates = broadcast(sql_context.read.parquet(path_to_exchange, schema=schema)
                               .select(from_unixtime(unix_timestamp('date', 'HH-dd-MM-yyyy')).alias('date'),
                                       'curr_long', 'curr_short', 'factor'))
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
    schema = StructType([StructField('id', StringType(), True),
                         StructField('name', StringType(), True),
                         StructField('motel_country', StringType(), True),
                         StructField('url', StringType(), True),
                         StructField('comment', StringType(), True)])
    motels = broadcast(sql_context.read.parquet(path_to_motels, schema=schema))
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
    return df_ranked.filter(df_ranked.rank == 1).select('MotelId', 'name', 'BidDate', 'country', expr('ROUND(price, 2)'))


if __name__ == '__main__':
    conf = SparkConf().setMaster('yarn').setAppName('test')
    conf.set('spark.executor.memory', '1g')
    conf.set('spark.driver.memory', '1g')
    sc = SparkContext(conf=conf)
    sql_sc = sql.HiveContext(sc)

    raw_unclear_bids = sql_sc.read.parquet(INPUT_DIR + 'bids.parquet', schema=SCHEMA)
    error_bids = get_errors_df(raw_unclear_bids)
    error_bids.write.csv(OUTPUT_DIR + 'error_bids')

    bids = get_clear_df(raw_unclear_bids)
    bids = get_eur_bids_df(bids, INPUT_DIR + 'exchange_rate.parquet', sql_sc)
    bids.write.csv(OUTPUT_DIR + 'bids_euro')

    bids = get_motels_names_df(bids, INPUT_DIR + 'motels.parquet', sql_sc)
    bids.write.csv(OUTPUT_DIR + 'bids_motels')

    bids = get_max_bids_df(bids)
    bids.write.csv(OUTPUT_DIR + 'bids')
    sc.stop()
