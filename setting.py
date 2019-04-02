import os

spark_master = "local[*]"

project_dir = os.path.dirname(os.path.realpath(__file__))
data_dir = os.path.join(project_dir, "data")

data_dir_raw = os.path.join(data_dir, "raw")
data_dir_interim = os.path.join(data_dir, "interim")
data_dir_processed = os.path.join(data_dir, "processed")
data_dir_external = os.path.join(data_dir, "external")

test_data_dir = os.path.join(project_dir, "src", "tests", "data")

figures_dir = os.path.join(project_dir, "reports", "figures")

ml_model_dir = os.path.join(data_dir, "processed", "ml_model")

nyc_raw_csv_filename = "nyc_raw.csv"
raw_train_filename = "df_raw_train.parquet"
raw_test_filename = "df_raw_test.parquet"

batch_filename_parquet =\
    "nyc_batch.parquet"  # filenames for sample batch prediction files
batch_filename_csv = "nyc_batch.csv"

batch_predictions_outfilename = \
    "nyc_batch_predictions.parquet"  # filename for batch prediction output

log_filename = "nyc_log.log"  # stored in data/interim

train_date_start = "2017-01-01"
train_date_end = "2017-01-31"
test_date_start = "2017-02-01"
test_date_end = "2017-02-28"
