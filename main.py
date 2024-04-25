# read files from gcp
import pandas as pd
from google.cloud import storage, bigquery
import re
import gcsfs
import json
from helpers import Logger, bigquery_to_pandas_types, send_telegram_message
import os
from dotenv import load_dotenv
from cloudevents.http import CloudEvent

import functions_framework


# load_dotenv()  

# Instantiates a client


def initialize(logger):
    elt_bucket = 'smarter-jobs'
    try:
        # with open("./etl_config.json") as f:
        #     cfg = json.load(f)        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(elt_bucket)
        blob = bucket.blob('config/etl_config.json')
        # Download the contents of the blob as a string and then parse it using json.loads() method
        cfg = json.loads(blob.download_as_string(client=None))
        print(f"Got config for {cfg['dataset_id']}")
        raw_folder = "ELT_raw"

        bigquery_client = bigquery.Client()
        bucket = storage_client.get_bucket(cfg["bucket"])
        dataset_ref = bigquery_client.dataset(cfg["dataset_id"], project=cfg["project"])
        return (
            cfg,
            raw_folder,
            elt_bucket,
            storage_client,
            bigquery_client,
            dataset_ref,
            bucket,
        )
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
        table_ref = dataset_ref.table(cfg["sources"][source_name]["bronze_table_name"])
        table = bigquery_client.get_table(table_ref)
        schema = [(x.name, x.field_type) for x in table.schema]
        columns = [x[0] for x in schema]
        return table_ref, table, schema, columns
    except Exception as e:
        logger.log(
            f"Unable to read {cfg['sources'][source_name]['bronze_table_name']} schema from bigquery: {e} ",
            level=50,
        )
        raise e


def read_file_into_df(cfg, source_name, file_path, logger):
    try:
        df = pd.read_csv(
            f"gs://{cfg['bucket']}/{file_path}",
            sep=cfg["sources"][source_name]["sep"],
            header=0,
        )
        return df
    except Exception as e:
        logger.log(f"unable to read {file_path} into pandas df: {e}", level=40)
        raise e


def validate_schema(df_raw, table_schema, logger):
    errors = 0
    raw_cols = [(col) for i, col in enumerate(df_raw.columns)]
    for table_col in table_schema:
        if table_col[0] not in raw_cols:
            logger.log(f"{table_col} not present in raw_cols", 40)
            errors += 1

    if errors >= 1:
        logger.log("Incompatable schema. unable to insert into table")
        logger.log(f"Table schema: {table_schema}")
        logger.log(f"Raw schema: {raw_cols}")

        print(f"RAW schema  |   Table schema")
        for i in range(max(len(raw_cols), len(table_schema))):
            print(
                f"{raw_cols[i] if i < len(raw_cols) else ''} {table_schema[i] if len(table_schema)>i else ''}"
            )
        raise Exception("Missing columns")
    else:
        logger.log("All necessary columns present")


def rename_columns(cfg, logger, df_raw, data_source):
    logger.log(f"renaming cols from config")
    try:
        rename_params = {
            col["old_name"]: col["new_name"]
            for col in cfg["sources"][data_source]["rename_columns"]
        }
        df_raw.rename(rename_params, axis=1, inplace=True)
        return df_raw
    except Exception as e:
        logger.log(f"Unable to rename cols: {e}")
        raise e


def select_cols(cfg, logger, df_raw, data_source, table_schema):
    logger.log("Casting data types to target table")
    try:
        for col, dtype in table_schema:
            if "DATE" in dtype:
                df_raw[col] = pd.to_datetime(df_raw[col])
            else:
                df_raw[col] = df_raw[col].astype(bigquery_to_pandas_types[dtype])
        return df_raw
    except Exception as e:
        logger.log(f"Unable to cast data types: {e}", level=40)
        raise e


def write_to_bigquery(bigquery_client, cfg, source_name, df, logger):
    logger.log("Attempting to write to GCP")

    table_id = cfg["sources"][source_name]["bronze_table_name"]
    full_table_id = f"{cfg['project']}.{cfg['dataset_id']}.{table_id}"

    job_config = bigquery.LoadJobConfig(
        # Specify a (partial) schema. All columns are always written to the
        # table. The schema is used to assist in data type definitions.
        schema=[
            # Specify the type of columns whose type cannot be auto-detected. For
            # example the "title" column uses pandas dtype "object", so its
            # data type is ambiguous.
            # bigquery.SchemaField("title", bigquery.enums.SqlTypeNames.STRING),
            # Indexes are written if included in the schema by name.
            # bigquery.SchemaField("wikidata_id", bigquery.enums.SqlTypeNames.STRING),
        ],
        # Optionally, set the write disposition. BigQuery appends loaded rows
        # to an existing table by default, but with WRITE_TRUNCATE write
        # disposition it replaces the table with the loaded data.
        # write_disposition="WRITE_TRUNCATE",
    )
    try:
        job = bigquery_client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )  # Make an API request.
        logger.log(f"Job result: {job.result()}")  # Wait for the job to complete.
    except Exception as e:
        logger.log(f"Unable to write to gcp table ({table_id})", 40)
        raise e


def move_to_processed_and_zip(storage_client, cfg, file_path, df, source_name, logger):
    file_name = file_path.rsplit("/")[1]
    zip_filename = file_name.split(".")[0] + ".zip"
    gcloud_zip_path = f"gs://{cfg['bucket']}/{cfg['processed_folder']}/{zip_filename}"
    logger.log(
        f"zipping file: {gcloud_zip_path} and writing to: {cfg['processed_folder']}"
    )
    # df.to_csv('gs://bucket/path')
    # upload the zip file to the processed folder
    try:
        compression_options = dict(method="zip", archive_name=f"{file_name}.txt")
        df.to_csv(
            gcloud_zip_path,
            sep=cfg["sources"][source_name]["sep"],
            index=False,
            compression=compression_options,
        )
    except Exception as e:
        logger.log("Unable to write compressed file to processed folder", 40)
        raise e

    try:
        logger.log(f"deleting {file_name} from {cfg['raw_folder']}")
        # delete the current file
        bucket = storage_client.bucket(cfg["bucket"])
        blob = bucket.blob(f"{cfg['raw_folder']}/{file_name}")
        generation_match_precondition = None

        # Optional: set a generation-match precondition to avoid potential race conditions
        # and data corruptions. The request to delete is aborted if the object's
        # generation number does not match your precondition.
        blob.reload()  # Fetch blob metadata to use in generation_match_precondition.
        generation_match_precondition = blob.generation

        blob.delete(if_generation_match=generation_match_precondition)
    except Exception as e:
        logger.log("Unable to delete folder from storage", 40)
        raise e
    logger.log("Successfully zipped file and moved to processed")


def process_data_source(
    storage_client, bigquery_client, dataset_ref, cfg, source_name, logger
):

    raw_files = get_file_paths(
        storage_client,
        cfg["bucket"],
        cfg["raw_folder"],
        cfg["sources"][source_name]["filename_regex"],
        logger,
    )

    table_ref, table, table_schema, table_columns = get_table_info(
        bigquery_client, dataset_ref, cfg, source_name, logger
    )

    for i, file_path in enumerate(raw_files):

        # comment this out for production
        # if i > 0:
        #     continue
        logger.log(f"Processing {file_path}")
        try:
            logger.increment_attempted()
            # read into pandas df
            df_raw = read_file_into_df(cfg, source_name, file_path, logger)
            df_raw_mod = rename_columns(cfg, logger, df_raw, source_name)
            validate_schema(df_raw, table_schema, logger)
            df_upload = select_cols(cfg, logger, df_raw_mod, source_name, table_schema)

            write_to_bigquery(bigquery_client, cfg, source_name, df_upload, logger)

            logger.increment_succeeded()
            move_to_processed_and_zip(
                storage_client, cfg, file_path, df_raw, source_name, logger
            )
            logger.log(f"Successfully processed {file_path}")

        except Exception as e:
            logger.log(f"Unable to process {file_path}: {e}")

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def run(cloud_event: CloudEvent) -> tuple:
# def run():
    logger = Logger()

    try:
        (
            cfg,
            raw_folder,
            bucket_name,
            storage_client,
            bigquery_client,
            dataset_ref,
            bucket,
        ) = initialize(logger)

    except Exception as e:
        # send message
        raise e

    for source in cfg["sources"]:
        # get list of files in the raw folder
        if cfg["sources"][source]["enabled"] != True:
            print(f"{source} not enabled")
            continue

        logger.log(f"processing source: {source}")
        try:
            process_data_source(
                storage_client, bigquery_client, dataset_ref, cfg, source, logger
            )
        except Exception as e:
            logger.log(f"Failed to process files for source: {source}: {e}", level=50)

    send_telegram_message(
        f"Finished ELT function: {logger.files_succeeded}/{logger.files_attempted} files succeeded",
        os.environ.get("TELEGRAM_TOKEN"),
        os.environ.get("TELEGRAM_CHAT_ID")
    )


# run()
