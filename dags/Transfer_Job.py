import pandas as pd
import logging

COLUMNS = ["Transaction_unique_identifier", "Price", "Date_of_Transfer", "Postcode",
           "Property_Type", "Old/New", "Duration", "PAON", "SAON", "Street", "Locality",
           "Town/City", "District", "County", "PPD_Category_Type", "Record_Status"]
PROPERTY_COLS = ["Postcode", "Property_Type", "PAON", "SAON", "Street", "Locality",
                 "Town/City", "District", "County"]
TRANSACTION_COLS = ["Transaction_unique_identifier", "Price", "Date_of_Transfer", "Old/New",
                    "Duration", "PPD_Category_Type", "Record_Status"]
TRANSACTION_DATA_NAME = "Transaction_data"

SOURCE_FILE = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv"
OUTPUT_FILE = ""


def read_and_transform_data_from_csv_to_json(source_csv):
    try:
        df = pd.read_csv(source_csv, header=None, names=COLUMNS)
        logging.debug(f"Successfully read data from {source_csv}")
        df = df.groupby(PROPERTY_COLS)
        df = df.apply(lambda x: x[TRANSACTION_COLS].to_dict('records'))
        df = df.reset_index()
        df = df.rename(columns={0: TRANSACTION_DATA_NAME})
        df.insert(0, "Property_Id", range(len(df)))
        logging.debug(f"Successfully processed file {source_csv}")
        return df.to_json(orient='records', lines=True)
    except Exception as e:
        logging.error(f"Could not transform data, error: {e}")
        raise


def transfer_records(source_file, target_file):
    logging.debug("Start transferring")
    df = read_and_transform_data_from_csv_to_json(source_file)
    try:
        with open(target_file, "a") as target_file:
            target_file.write(df)
    except Exception as e:
        logging.error(f"Could not write to target file: {target_file}, \n Error: {e}")
        raise


def run_job():
    transfer_records(SOURCE_FILE, OUTPUT_FILE)