# read files from gcp
import pandas as pd
from google.cloud import storage, bigquery
import re
import gcsfs
import json
from helpers import Logger

# Instantiates a client


def initialize(logger):
    try:
        with open("./etl_config.json") as f:
            cfg = json.load(f)

        raw_folder = "ELT_raw"
        bucket_name = "smarter-jobs"

        storage_client = storage.Client()
        bigquery_client = bigquery.Client()
        bucket = storage_client.get_bucket(cfg["bucket"])
        dataset_ref = bigquery_client.dataset(cfg['dataset_id'], project=cfg['project'])
        return cfg, raw_folder, bucket_name, storage_client, bigquery_client, dataset_ref, bucket
    except Exception as e:
        logger.log("Unable to initialize GCP clients", level=50)
        raise e


# df = pd.read_csv('gs://bucket/your_path.csv')
def get_file_paths(storage_client, bucket, folder, regex, logger):
    try:
        return [
            x.name
            for x in storage_client.list_blobs(bucket, prefix=folder)
            if len(re.findall(regex, x.name)) > 0
        ]
    except Exception as e:
        logger.log(f"Unable to list directory", level=50)
        raise e


def get_table_info(bigquery_client, dataset_ref, cfg, source_name, logger):
    try:
        table_ref = dataset_ref.table(cfg['sources'][source_name]['bronze_table_name'])
        table = bigquery_client.get_table(table_ref)
        schema = [(x.name, x.field_type) for x in table.schema]
        columns = [x[0] for x in schema]
        return table_ref, table, schema, columns
    except Exception as e:
        logger.log(f"Unable to read {cfg['sources'][source_name]['bronze_table_name']} schema from bigquery: {e} ", level=50)
        raise e

def read_file_into_df(cfg, source_name, file_path, logger):
    try:
        df= pd.read_csv(
                f"gs://{cfg['bucket']}/{file_path}", sep=cfg['sources'][source_name]['sep'], header=0
            )
        return df
    except Exception as e:
        logger.log(f"unable to read {file_path} into pandas df: {e}", level=40)
        raise e



def compare_schemas(target_schema, new_schema):
    pass


def process_data_source(storage_client, bigquery_client, dataset_ref, cfg, source_name, logger):
    
    raw_files = get_file_paths(
        storage_client,
        cfg["bucket"],
        cfg["raw_folder"],
        cfg['sources'][source_name]["filename_regex"],
        logger,
    )
    
    
    table_ref, table, table_schema, table_columns = get_table_info(bigquery_client, dataset_ref, cfg, source_name, logger)
    
    for i, file_path in enumerate(raw_files):
        logger.increment_attempted()
        
        if i > 0:
            continue
        try:
            # read into pandas df
            df_raw = read_file_into_df(cfg, source_name, file_path, logger)

            schema = [(col) for i, col in enumerate(df_raw.columns)]
            print(f"{file_path.split('/')[-1]}  |   {cfg['sources'][source_name]['bronze_table_name']}")
            for i in range(max(len(schema), len(table_schema))):
                print(f"{schema[i] if i < len(schema) else ''} {table_schema[i] if len(table_schema)>i else ''}")
            
            print(schema)
            print(f"{file_path} schema: {df_raw.shape}")

            logger.increment_succeeded()
        except Exception as e:
            logger.log(f"Unable to process ${file_path}: {e}")


def run():
    logger = Logger()

    try:
        cfg, raw_folder, bucket_name, storage_client, bigquery_client, dataset_ref, bucket = initialize(
            logger
        )
    except Exception as e:
        # send message
        raise e

    for source in cfg["sources"]:
        # get list of files in the raw folder
        if cfg["sources"][source]["enabled"]!=True:
            print(f"{source} not enabled")
            continue
        
        logger.log(f"processing source: {source}")
        try:
            process_data_source(storage_client, bigquery_client, dataset_ref, cfg, source, logger)
        except Exception as e:
            logger.log(f"Failed to process files for source: {source}: {e}", level=50)



run()
