import setting
import os
from pyspark.mllib.evaluation import RegressionMetrics
from src.spark_session import spark
from src.features import build_features
from src.models import train_model

from src.utils import log_config
logger = log_config.get_logger(__name__)


def evaluate_model():
    '''
    will read train and test files from jan and Feb 2017 to evaluate model
    :return:
    '''
    ml_model = train_model.EnsembleModel()
    df_raw_train_filepath = os.path.join(setting.data_dir_interim, setting.raw_train_filename)
    df_raw_test_filepath = os.path.join(setting.data_dir_interim, setting.raw_test_filename)
    logger.info("using data from {} for training and validation".format(df_raw_train_filepath))
    logger.info("using data from {} for testing".format(df_raw_test_filepath))
    df_raw_train = spark.read.parquet(df_raw_train_filepath)
    df_raw_test = spark.read.parquet(df_raw_test_filepath)

    train_frac = 0.75
    test_frac = (1 - train_frac)

    df_raw_train, df_raw_val = df_raw_train.randomSplit([train_frac, test_frac])

    df_train = build_features.featurize(df_raw_train)

    ml_model = ml_model.fit(df_train)

    _, val_predictions = ml_model.transform(build_features.featurize(df_raw_val))
    _, test_predictions = ml_model.transform(build_features.featurize(df_raw_test))

    val_prediction_labels = val_predictions.select("tip_amount", "prediction").rdd
    val_test_metrics = RegressionMetrics(val_prediction_labels)
    test_prediction_labels = test_predictions.select("tip_amount", "prediction").rdd
    test_test_metrics = RegressionMetrics(test_prediction_labels)

    logger.info("Validation set RMSE = {}".format(val_test_metrics.rootMeanSquaredError))
    logger.info("Test set RMSE = {}".format(test_test_metrics.rootMeanSquaredError))


if __name__ == '__main__':
    evaluate_model()
