
from src.models import train_model
from src.features import build_features
import setting
import argparse
from src.utils import log_config
from src.data import make_dataset
from os.path import join as os_join
logger = log_config.get_logger(__name__)


def _batch_predict_and_save(df, ml_model,
                            batch_predictions_outfilename,
                            storage_dir_out):
    """
    runs batch predictions and saves output in parquet files (use as private method)
    :param df:
    :param ml_model:
    :param batch_predictions_outfilename:
    :param storage_dir_out:
    :return:
    """
    df_featurized = build_features.featurize(df)
    ml_model, predictions = ml_model.transform(df_featurized)

    batch_predictions_outfilepath = os_join(storage_dir_out,
                                            batch_predictions_outfilename)
    logger.info("writing batch predictions "
                "to {}".format(batch_predictions_outfilepath))

    predictions.write.parquet(batch_predictions_outfilepath, mode="overwrite")


def batch_predict_and_save(
        csv_infilename=setting.batch_filename_csv,
        storage_dir_infile=setting.data_dir_interim,
        batch_predictions_outfilename=setting.batch_predictions_outfilename,
        storage_dir_out=setting.data_dir_processed):
    """
    runs batch predictions and saves output in parquet files
    :param csv_infilename: Input filename
    :param storage_dir_infile: storage directory for input file
    :param batch_predictions_outfilename: output filename
    :param storage_dir_out: storage directory for output file
    :return: None
    """
    df = make_dataset.ingest_raw_csv(raw_csv_filename=csv_infilename,
                                     storage_dir=storage_dir_infile,
                                     tip_amount_present=False,
                                     cleanup=True)
    logger.info("input file for batch predictions: "
                "{}".format(os_join(storage_dir_infile, csv_infilename)))
    ml_model = train_model.EnsembleModel(load_existing=True)

    _batch_predict_and_save(
        df=df,
        batch_predictions_outfilename=batch_predictions_outfilename,
        storage_dir_out=storage_dir_out, ml_model=ml_model)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--infilename", "-ifn", action="store",
                        help="Name of input CSV file for "
                        "batch prediction. Make sure to store it in"
                        "data/interim folder. Expected format is vendor_id, "
                        "pickup_datetime,dropoff_datetime, "
                        "store_and_fwd_flag,ratecode_id,pu_location_id, "
                        "do_location_id,passenger_count,trip_distance, "
                        "fare_amount,extra,mta_tax,tolls_amount,ehail_fee, "
                        "improvement_surcharge,total_amount,payment_type, "
                        "trip_type")

    parser.add_argument(
        "--outfilename", "-ofn",
        action="store",
        help="Name of output file for batch predictions. "
             "File will be stored in "
             "data/processed folder.")

    args = parser.parse_args()

    infilename = setting.batch_filename_csv
    outfilename = setting.batch_predictions_outfilename

    if args.infilename:
        infilename = args.infilename
    if args.outfilename:
        outfilename = args.outfilename

    batch_predict_and_save(csv_infilename=infilename,
                           batch_predictions_outfilename=outfilename)


if __name__ == '__main__':
    main()
