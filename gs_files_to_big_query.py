# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 13:56:50 2022

@author: mthulisi.moyo
"""

import os
import logging
from google.cloud import bigquery
from google.cloud import storage
from gs_files_to_gcs import *
from datetime import timedelta, datetime
import argparse

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "secrets/credentials.json"
storage_client = storage.Client.from_service_account_json(
    'secrets/credentials.json')


def bq_insert_row_into_table(table_id, rows_to_insert, selected_fields):
    client = bigquery.Client()
    target_table = client.get_table(table_id)
    errors = client.insert_rows(target_table, rows_to_insert, selected_fields)
    if len(errors) > 0:
        print("Insert errors:,", errors)


def is_loaded_record(table_id, file_name, file_date):
    client = bigquery.Client()
    query = 'SELECT is_loaded FROM `{}` WHERE file_name = "{}" AND file_business_date = "{}" AND is_loaded = TRUE LIMIT 1'.format(table_id, file_name, file_date)
    try:
        query_job = client.query(query)
        bq_result = [row for row in query_job.result()]
        if len(bq_result) >= 1:
            if bq_result[0]:
                return "processed"
            else:
                return "not processed"
        else:
            return "not loaded"
    except Exception as e:
        print(f"Error occured: {e}")
        # TODO: to process exception
        raise


def bq_load_table_from_gcs(uri, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        autodetect=False, source_format=bigquery.SourceFormat.CSV,
        field_delimiter="|", max_bad_records=1, skip_leading_rows=2)
    try:
        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config)
        load_job.result()
        return load_job
    except Exception as e:
        print(f"Error occured: {e}")
        return load_job


def gcs_move_blob(source_bucket, blob_name, destination_bucket_name):
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.get_bucket(destination_bucket_name)
    source_bucket.copy_blob(source_blob, destination_bucket, blob_name)
    source_blob.delete()


def processor(source_bucket, destination_bucket, run_date):
    log_table_id = "ao-prototype.gs_staging.gs_file_load_log"
    bucket = storage_client.get_bucket(source_bucket)
    table_id = ""

    for blob in bucket.list_blobs(prefix=run_date):
        if blob.name.find("poapacc") > 0:
            table_id = "ao-prototype.gs_staging.poapacc"
        elif blob.name.find("tdact") > 0:
            table_id = "ao-prototype.gs_staging.tdact"
        else:
            raise Exception(f"Unknown file type {blob.name}")

        uri = f"gs://{bucket.name}/{blob.name}"
        current_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Processing {uri} at {current_time}")
        file_name = blob.name.split("/")[3]
        downloaded_file = blob.download_as_text(encoding="utf-8")
        header = downloaded_file.splitlines()[0]
        footer = downloaded_file.splitlines()[-1]
        metadata = get_file_metadata(file_name, header, footer)
        file_business_date = metadata[0][5]
        # raise
        if is_loaded_record(log_table_id, file_name,
                            file_business_date) == "not processed":
            job = bq_load_table_from_gcs(uri=uri, table_id=table_id)
            client = bigquery.Client()
            if job.output_rows is not None:
                query =  'UPDATE `{log_table_id}` SET log_detail = "loaded {job.output_rows} rows", is_loaded= TRUE WHERE file_name = "{file_name}" AND file_business_date = "{file_business_date}"'
                client.query(query)
                gcs_move_blob(bucket, blob.name, destination_bucket)
            else:
                query =  'UPDATE `{log_table_id}` SET log_detail = "{job.error_result.get("message")}", is_loaded= FALSE WHERE file_name = "{file_name}" AND file_business_date = "{file_business_date}"'
                client.query(query)
        elif is_loaded_record(log_table_id, file_name,
                              file_business_date) == "not loaded":
            job = bq_load_table_from_gcs(uri=uri, table_id=table_id)
            schema = [bigquery.SchemaField("file_report_item_name", "STRING"),
                      bigquery.SchemaField("file_report_item_id", "STRING"),
                      bigquery.SchemaField("file_run_date", "STRING"),
                      bigquery.SchemaField("file_fund_id", "STRING"),
                      bigquery.SchemaField("file_advisor", "STRING"),
                      bigquery.SchemaField("file_business_date", "STRING"),
                      bigquery.SchemaField("file_record_count", "INTEGER"),
                      bigquery.SchemaField("file_name", "INTEGER"),
                      bigquery.SchemaField("is_loaded", "BOOL"),
                      bigquery.SchemaField("log_detail", "STRING")]
            if job.output_rows is not None:
                log_detail = f'loaded {job.output_rows} rows'
                metadata_to_insert = [metadata[0]+(True, log_detail,)]
                bq_insert_row_into_table(log_table_id,
                                         metadata_to_insert, schema)
                gcs_move_blob(bucket, blob.name, destination_bucket)
            else:
                log_detail = f'Error: {job.error_result.get("message")}'
                metadata_to_insert = [metadata[0]+(False, log_detail,)]
                bq_insert_row_into_table(log_table_id,
                                         metadata_to_insert, schema)
        elif is_loaded_record(log_table_id, file_name,
                              file_business_date) == "processed":
            print(f'File ({blob.name}) for ({file_business_date}) was already loaded')
            gcs_move_blob(bucket, blob.name, destination_bucket)
        else:
            print("something else went wrong")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    run_date = datetime.now() - timedelta(1)
    parser.add_argument("-r", "--run_date", default=run_date.strftime("%Y-%m-%d"),
                        help="enter a run date in the format YYYY-MM-dd")
    parser.add_argument("-d", "--days_ago", default=5, type=int,
                        help="how many days back should the app run (inclusive)")
    parser.add_argument("-src", "--source", default="gs-incoming-files",
                        help="enter GCP bucket name with the files")
    parser.add_argument("-dest", "--destination", default="gs-loaded-files",
                        help="enter GCP archival bucket name for the files")
    args = parser.parse_args()

    end = datetime.strptime(args.run_date, "%Y-%m-%d")
    start = end - timedelta(args.days_ago)
    date_generated = [start + timedelta(days=x)
                      for x in range(0, (end-start).days + 1)]
    for report_date in date_generated:
        run_date = report_date.strftime("%Y/%m/%d/")
        print(f"looking for files in: gs://{args.source}/{run_date}")
        processor(args.source, args.destination, run_date)
