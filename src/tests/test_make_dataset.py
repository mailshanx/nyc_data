import unittest
from src.spark_session import spark
from src.data import make_dataset
import setting
import os
import pickle
from src.utils import utils
from pandas.testing import assert_frame_equal


class PySparkTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = spark

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class MakeDatasetTest(PySparkTest):

    def test_ingest_raw_csv(self):
        '''
        Tests spark ingest of raw csv file downloaded from NYC website
        :return:
        '''
        raw_csv_sample = os.path.join(setting.test_data_dir, "nyc_raw_sample.csv")
        df_raw = make_dataset.ingest_raw_csv(raw_csv_filename=raw_csv_sample)
        # df_raw_pd = pickle.load(open(os.path.join(setting.test_data_dir, "df_raw_sample_pd.pkl"), "rb"))
        line_cnt = utils.line_cnt(raw_csv_sample)
        self.assertEqual(df_raw.count(), line_cnt-1)
        self.assertEqual(df_raw.columns, ['vendor_id', 'pickup_datetime', 'dropoff_datetime', 'store_and_fwd_flag',
                                          'ratecode_id', 'pu_location_id', 'do_location_id', 'passenger_count', 'trip_distance',
                                          'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                                          'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type'])
        #assert_frame_equal_with_sort(df_raw.toPandas(), df_raw_pd, "pickup_datetime")


    def test_make_sample_batch_csv(self):
        '''
        Tests sample batch file generation
        :return:
        '''
        storage_dir, filename_csv, filename_parquet = make_dataset.make_sample_batch_csv()
        nyc_batch = make_dataset.ingest_raw_csv(raw_csv_filename=filename_csv, storage_dir=storage_dir,
                                                tip_amount_present=False, cleanup=True)
        nyc_batch_pd = utils.parquet_dir_to_pandas_df(os.path.join(storage_dir, filename_parquet))
        self.assertEqual(nyc_batch.count(), len(nyc_batch_pd))
        expected_columns = ['do_location_id', 'dropoff_datetime', 'extra', 'fare_amount',
                            'improvement_surcharge', 'mta_tax', 'passenger_count', 'payment_type',
                            'pickup_datetime', 'pu_location_id', 'ratecode_id', 'store_and_fwd_flag',
                            'tolls_amount', 'total_amount', 'trip_distance', 'trip_type', 'vendor_id']
        for actual_col, expected_col in zip(sorted(nyc_batch.columns), sorted(expected_columns)):
            self.assertEqual(actual_col, expected_col)


def assert_frame_equal_with_sort(results, expected, keycolumns):
    results_sorted = results.sort_values(by=keycolumns).reset_index(drop=True)
    expected_sorted = expected.sort_values(by=keycolumns).reset_index(drop=True)
    assert_frame_equal(results_sorted, expected_sorted)
    pass


if __name__ == '__main__':
    unittest.main()
