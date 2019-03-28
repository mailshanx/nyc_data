
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
from src.features.build_features import Featurizer
from pyspark.ml import PipelineModel
import setting
import os
import pyspark.sql.functions as f
from src.spark_session import spark
from src.features import build_features
from src.utils import log_config
logger = log_config.get_logger(__name__)

class EnsembleModel:
    '''
    Main class to manage ensemble models
    '''
    def __init__(self, load_existing=False, path=setting.ml_model_dir):
        self.models = {}
        self._namelist = ["enet", "gbtree"]
        if load_existing:
            self.load(path=path)
        else:
            self.enet = LinearRegression(featuresCol="indexed_features", maxIter=25, labelCol="tip_amount",
                                         regParam=0.05, elasticNetParam=0.5)
            self.gbtree = GBTRegressor(featuresCol="indexed_features", labelCol="tip_amount",
                                       maxIter=10, maxBins=270)

            self.models = {"enet": Pipeline(stages=[Featurizer().featurizer, self.enet]),
                           "gbtree": Pipeline(stages=[Featurizer().featurizer, self.gbtree])}

    def fit(self, df):
        for name, model in self.models.items():
            self.models[name] = model.fit(df)

        return self

    def transform(self, df):
        first_model_name = self._namelist[0]
        old_predictions = self.models[first_model_name].transform(df)
        old_predictions = old_predictions.select(*df.columns, "prediction")
        old_predictions = old_predictions.withColumnRenamed("prediction", "prediction_"+first_model_name)
        for i in range(1, len(self._namelist)):
            model_name = self._namelist[i]
            new_predictions = self.models[model_name].transform(old_predictions)
            new_predictions = new_predictions.select(*old_predictions.columns, "prediction")
            new_predictions = new_predictions.withColumnRenamed("prediction", "prediction_"+model_name)
            old_predictions = new_predictions
        col_list = [f.col("prediction_gbtree"), f.col("prediction_enet")]
        assert len(col_list) == len(self._namelist)
        #store mean of predictions
        old_predictions = old_predictions.withColumn("prediction", (col_list[0] + col_list[1])/len(col_list))
        return self, old_predictions

    def save(self, path=setting.ml_model_dir):
        for name, model in self.models.items():
            model_save_path = os.path.join(path, name)
            model.write().overwrite().save(model_save_path)
        return self

    def load(self, path=setting.ml_model_dir):
        dirs = os.listdir(path)
        for dirname in dirs:
            if dirname in self._namelist:
                model_save_path = os.path.join(path, dirname)
                self.models[str(dirname)] = PipelineModel.load(model_save_path)
        return self


def train_and_save(path=setting.ml_model_dir):
    '''
    trains and saves a model on January 2017 data
    :param path:
    :return:
    '''
    df_raw_train_filepath = os.path.join(setting.data_dir_interim, setting.raw_train_filename)

    logger.info("using data stored in {} to train and save model in {}".format(df_raw_train_filepath, path))
    df_raw_train = spark.read.parquet(df_raw_train_filepath)

    ml_model = EnsembleModel()

    df_raw_train_featurized = build_features.featurize(df_raw_train)

    ml_model = ml_model.fit(df_raw_train_featurized)

    ml_model = ml_model.save(path=path)

    return ml_model


if __name__ == '__main__':
    train_and_save()
