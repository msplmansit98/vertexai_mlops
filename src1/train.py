import kfp.v2.dsl, kfp.v2.compiler
from kfp.v2.dsl import Artifact, Dataset, Input, Metrics, Model, Output


# Step 2. Defining Data Path
dataset_input_file_path = "C:\\Users\\ShadabHussain\\OneDrive - TheMathCompany Private Limited\\Documents\\MLOps\\vertexai_mlops\\dataset\\processed_dataset\\TMC_ALL_SKU_ADS_2016_DAY_LEVEL.csv"


@kfp.v2.dsl.component(base_image="python:3.9-slim", packages_to_install=["pandas", "statsmodels"])
def train_model(input: Input[Dataset], model: Output[Model], metrics: Output[Metrics]):
    # Step 1. Importing Packages
    import pandas as pd
    import pickle
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    # Step 3. Load Data
    df = pd.read_csv(input.path)[df.sku=='10491-0']

    # Defining Variables
    train_no = 42
    p=1
    d=0
    q=3
    P=0
    D=0
    Q=0
    T=3

    # Step 4.1 Data Filter
    date_col = 'calendar_date'
    dep_col='sales_qty'
    df.set_index(date_col,inplace=True)
    train = df[:train_no]
    test = df[train_no:]
    start = len(train)
    end = len(df)-1
    

    # Step 4. Model Training
    ml_model = SARIMAX(train[dep_col],order = (p,d,q),seasonal_order =(P,D,Q,T))
    result = ml_model.fit()
    prediction = result.predict(start,end,order=(p,d,q),seasonal_order=(P,D,Q,T)).rename("predictions")
    pred = pd.DataFrame(prediction)
    pred.reset_index(inplace=True)
    pred.rename(columns={'index':date_col},inplace=True)


    # Step 4.2 Save Model
    with open(model.path, "wb") as f:
        pickle.dump(ml_model, f)

    # Step 4.3 Model Metrics
    # accuracy = ml_model.score(x_test, y_test)
    # metrics.log_metric("accuracy", (accuracy * 100.0))
    # metrics.log_metric("framework", "Scikit Learn")

    