# Step 1. Importing Packages
import pandas as pd
import kfp.v2.dsl, kfp.v2.compiler
from kfp.v2.dsl import Artifact, Dataset, Input, Output

# Step 2. Defining Data Path
dataset_input_file_path = "C:\\Users\\ShadabHussain\\OneDrive - TheMathCompany Private Limited\\Documents\\MLOps\\vertexai_mlops\\data\\interim_dataset\\TMC_ALL_SKU_ADS_2016_DAY_LEVEL.xlsx"
dataset_output_file_path = "C:\\Users\\ShadabHussain\\OneDrive - TheMathCompany Private Limited\\Documents\\MLOps\\vertexai_mlops\\data\\processed_dataset\\TMC_ALL_SKU_ADS_2016_DAY_LEVEL.csv"

@kfp.v2.dsl.component(base_image="python:3.9-slim", packages_to_install=["pandas", "statsmodels"])
def features(input: Input[Dataset], output: Output[Dataset]):
    # Step 3. Load Data
    df = pd.read_csv(input.path)

    # Step 4. Features Prep

    # Step 4.1 Creating Date from CALENDAR_YEAR_MONTH column
    df["CALENDAR_YEAR_MONTH"] = df["CALENDAR_YEAR_MONTH"].map(lambda d: str(d))
    df["calendar_date"] = df["CALENDAR_YEAR_MONTH"].map(lambda d: d[0:4]+'-'+d[4:6]+'-'+'01')

    # Step 4.2 Rename columns
    df.rename(columns = {'QTY_ORDER_CHANGE':'sales_qty',
                        'MATERIAL':'sku',
                        'USD_ITEM_VALUE_NET':'sales_usd',
                        'CALENDAR_YEAR_MONTH':'calendar_year_month',}, inplace = True)

    # Step 4.3. Save Processed Data
    df.to_csv(output.path, index=False)