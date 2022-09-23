import kfp.v2.dsl, kfp.v2.compiler
from kfp.v2.dsl import Artifact, Dataset, Input, Metrics, Model, Output


@kfp.v2.dsl.pipeline(name="arima-pipeline")
def my_pipeline():
    from data_prep import load_data
    from features_prep import features
    from train import train_model
    raw_data = load_data()
    features = features(raw_data.outputs["output"])
    train_model(features.outputs["output"])