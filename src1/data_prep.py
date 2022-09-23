
import kfp.v2.dsl, kfp.v2.compiler
from kfp.v2.dsl import Dataset, Output


@kfp.v2.dsl.component(base_image="python:3.9-slim", packages_to_install=["pandas", "statsmodels"])
def load_data(output: Output[Dataset]):
    # Step 1. Importing Packages
    import pandas as pd

    # Step 2. Defining Data Path
    dataset_input_file_path = "C:\\Users\\ShadabHussain\\OneDrive - TheMathCompany Private Limited\\Documents\\MLOps\\vertexai_mlops\\data\\raw_dataset\\TMC_ALL_SKU_ADS_2016_DAY_LEVEL.xlsx"
    dataset_output_file_path = "C:\\Users\\ShadabHussain\\OneDrive - TheMathCompany Private Limited\\Documents\\MLOps\\vertexai_mlops\\data\\interim_dataset\\TMC_ALL_SKU_ADS_2016_DAY_LEVEL.csv"

    # Step 3. Load Data
    df = pd.read_excel(dataset_input_file_path)

    # Step 4. Data Cleaning

    # Step 4.1 Removing trailing "-" from MATERIAL column
    df["MATERIAL"] = df["MATERIAL"].str.rstrip("-")

    # Step 4.2 Aggregating Transactional Data at Month Level
    df = df[["MATERIAL", "USD_ITEM_VALUE_NET", 
    "QTY_ORDER_CHANGE", "CALENDAR_YEAR_MONTH"]].groupby(
        ["CALENDAR_YEAR_MONTH","MATERIAL"]).sum().reset_index()

    # Step 4.3. Save Interim Data
    df.to_csv(output.path, index=False)

