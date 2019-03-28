# -*- coding: utf-8 -*-
import os
import logging
import argparse
import datetime
import pyspark
import pandas as pd
import pickle
import numpy as np
from pyspark.sql import types as t
from pyspark.sql import functions as f
from src.spark_session import spark
import setting
from src.utils import log_config, utils
logger = log_config.get_logger(__name__)


def ingest_raw_csv(raw_csv_filename=setting.nyc_raw_csv_filename,
                   storage_dir=setting.data_dir_raw,
                   cleanup=True, tip_amount_present=True):
    '''
    Ingests raw CSV file to pyspark dataframe
    :param raw_csv_filename:
    :param storage_dir:
    :param cleanup:
    :param tip_amount_present:
    :return:
    '''
    nyc_raw_csv_filepath = os.path.join(storage_dir, raw_csv_filename)
    logger.info("ingesting raw csv file from {}".format(nyc_raw_csv_filepath))
    df_raw = spark.read.csv(path=nyc_raw_csv_filepath, header=True, inferSchema=True)
    if tip_amount_present:
        df_col_names = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
                        'store_and_fwd_flag', 'ratecode_id', 'pu_location_id', 'do_location_id',
                        'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
                        'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge',
                        'total_amount', 'payment_type', 'trip_type']
    else:
        df_col_names = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
                        'store_and_fwd_flag', 'ratecode_id', 'pu_location_id', 'do_location_id',
                        'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax',
                        'tolls_amount', 'ehail_fee', 'improvement_surcharge',
                        'total_amount', 'payment_type', 'trip_type']
    df_raw = df_raw.toDF(*df_col_names)
    df_raw = df_raw.withColumn("pickup_datetime", f.to_timestamp(df_raw.pickup_datetime, 'MM/dd/yyyy hh:mm:ss a'))
    df_raw = df_raw.withColumn("dropoff_datetime", f.to_timestamp(df_raw.dropoff_datetime, 'MM/dd/yyyy hh:mm:ss a'))
    if cleanup:
        # drop ehail fee, it is null in the entire dataset
        df_raw = df_raw.drop('ehail_fee')
    logger.info("ingested and cleaned raw csv file {}".format(nyc_raw_csv_filepath))
    return df_raw


def filter_and_persist_train_test_raw(df_raw):
    '''
    Filters and saves the January and February datasets
    :param df_raw:
    :return:
    '''
    df_raw_train_filepath = os.path.join(setting.data_dir_interim, setting.raw_train_filename)
    df_raw_test_filepath = os.path.join(setting.data_dir_interim, setting.raw_test_filename)
    df_raw_train, df_raw_test = filter_train_test(df_raw=df_raw)
    logger.info("writing raw train and test files to {} and {}".format(df_raw_train_filepath, df_raw_test_filepath))
    df_raw_train.write.parquet(path=df_raw_train_filepath, mode="overwrite")
    df_raw_test.write.parquet(path=df_raw_test_filepath, mode="overwrite")


def filter_train_test(df_raw):
    train_date_start = pd.to_datetime(setting.train_date_start)
    train_date_cutoff = pd.to_datetime(setting.train_date_end) + datetime.timedelta(days=1)

    test_date_start = pd.to_datetime(setting.test_date_start)
    test_date_cutoff = pd.to_datetime(setting.test_date_end) + datetime.timedelta(days=1)

    df_raw_train = filter_by_dates(df_raw, "pickup_datetime", train_date_start, train_date_cutoff)
    df_raw_test = filter_by_dates(df_raw, "pickup_datetime", test_date_start, test_date_cutoff)
    return df_raw_train, df_raw_test


def make_sample_batch_csv(storage_dir=setting.data_dir_interim,
                          filename_parquet=setting.batch_filename_parquet,
                          filename_csv=setting.batch_filename_csv, fraction=0.001):
    '''
    Generates sample batch file for testing batch predictions
    :param storage_dir:
    :param filename_parquet:
    :param filename_csv:
    :param fraction:
    :return:
    '''
    nyc_raw = ingest_raw_csv(raw_csv_filename=setting.nyc_raw_csv_filename, cleanup=False)
    nyc_raw_sample = nyc_raw.sample(withReplacement=False, fraction=fraction)
    nyc_batch = nyc_raw_sample.drop('tip_amount')
    nyc_batch_filepath_parquet = os.path.join(storage_dir, filename_parquet)
    nyc_batch_filepath_csv = os.path.join(storage_dir, filename_csv)
    logger.info("writing batch file in parquet format to {}".format(nyc_batch_filepath_parquet))
    nyc_batch.write.parquet(nyc_batch_filepath_parquet, mode="overwrite")
    nyc_batch_pd = utils.parquet_dir_to_pandas_df(nyc_batch_filepath_parquet)
    logger.info("writing batch file in csv format to {}".format(nyc_batch_filepath_csv))
    nyc_batch_pd.to_csv(nyc_batch_filepath_csv, index=False)
    return storage_dir, filename_csv, filename_parquet


def filter_by_dates(df, col_name, date_start, date_cutoff):
    assert date_start <= date_cutoff
    df_date_filtered = df.filter((f.col(col_name) >= date_start) & (f.col(col_name) < date_cutoff))
    return df_date_filtered


def remove_outliers(df_raw):
    '''
    Removes outliers
    :param df_raw:
    :return:
    '''
    diff_secs_col = f.col("dropoff_datetime").cast("long") - f.col("pickup_datetime").cast("long")
    df_raw = df_raw.withColumn("trip_duration_m", diff_secs_col / 60.0)
    df_raw = df_raw.filter((f.col("tip_amount") >= 0) & (f.col("tip_amount") < 20)
                           & (f.col("trip_duration_m") > 0) & (f.col("trip_duration_m") < 180))
    return df_raw


def main():
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../interim).
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--rawingest", action="store_true",   help="ingest raw csv file")
    parser.add_argument("--rawpreprocess", "-rpp",   action="store_true",
                        help="Ingest raw csv data and generate raw train and test data by "
                        "filtering by date. Raw train and test files are stored in {} and "
                        "{} in the data/interim folder ".format(setting.raw_train_filename,
                                                                setting.raw_test_filename))
    parser.add_argument("--batchgen", action="store_true", help="generate sample batch file for "
                                                                "testing predictions")
    args = parser.parse_args()
    if args.rawingest:
        df_raw = ingest_raw_csv()
    elif args.rawpreprocess:
        df_raw = ingest_raw_csv()
        filter_and_persist_train_test_raw(df_raw=df_raw)
    if args.batchgen:
        make_sample_batch_csv()


if __name__ == '__main__':
    main()
