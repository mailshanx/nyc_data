from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.feature import RFormula
import pyspark.sql.functions as f


class StringEncoder:

    def __init__(self):
        self.sI1 = StringIndexer(inputCol="ratecode_id",
                                 outputCol="ratecode_index",
                                 handleInvalid="keep")
        self.sI2 = StringIndexer(inputCol="vendor_id",
                                 outputCol="vendor_index",
                                 handleInvalid="keep")
        self.sI3 = StringIndexer(inputCol="payment_type",
                                 outputCol="payment_type_index",
                                 handleInvalid="keep")
        self.sI4 = StringIndexer(inputCol="store_and_fwd_flag",
                                 outputCol="store_and_fwd_flag_index",
                                 handleInvalid="keep")
        self.indexers = [self.sI1, self.sI2, self.sI3, self.sI4]

        self.string_idx_model = Pipeline(stages=self.indexers)


class FormEncoder:

    def __init__(self, formula="tip_amount ~ passenger_count + \
                        fare_amount + vendor_index + ratecode_index \
                        + trip_duration_m + store_and_fwd_flag_index + \
                        trip_type + pu_location_id + do_location_id + \
                        trip_distance"):
        self.reg_formula = RFormula(formula=formula)

        self.feature_indexer = VectorIndexer(inputCol="features",
                                             outputCol="indexed_features",
                                             handleInvalid="keep",
                                             maxCategories=270)
        self.indexers = [self.reg_formula, self.feature_indexer]
        self.form_encoder_model = Pipeline(stages=self.indexers)


class Featurizer:

    def __init__(self):
        self.string_encoder = StringEncoder().string_idx_model
        self.form_encoder = FormEncoder().form_encoder_model
        self.featurizer_stages = [self.string_encoder, self.form_encoder]
        self.featurizer = Pipeline(stages=self.featurizer_stages)


def featurize(df):
    """
    featurizes raw dataframe
    :param df:
    :return:
    """
    df = featurize_trip_duration(df)
    return df


def featurize_trip_duration(df):
    """
    trip duration as a feature
    :param df:
    :return:
    """
    diff_secs_col = \
        f.col("dropoff_datetime").cast("long") \
        - f.col("pickup_datetime").cast("long")
    df = df.withColumn("trip_duration_m", diff_secs_col / 60.0)
    return df
