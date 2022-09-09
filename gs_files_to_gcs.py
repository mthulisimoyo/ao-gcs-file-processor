import os
import argparse
import logging
from google.cloud import storage
from platform import system
from datetime import timedelta, datetime
from google.cloud import bigquery
from gs_files_to_big_query import *

logging.basicConfig(level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

FILES = [
    r"SRTCS_209751_1200357639_tdact_GOG_111.dat",
    r"SRTCS_221646_1200623495_tdact_GOG_111.dat",
    r"SRTCS_209751_1200357628_poapacc_01323945_116.dat",
    r"SRTCS_209751_1200357627_poapacc_01323944_116.dat",
    r"SRTCS_221646_1200623465_poapacc_03530714_116.dat",
    r"SRTCS_221646_1200623469_poapacc_06424423_116.dat",
    r"SRTCS_209751_1200467928_poapacc_04954564CURACAOHOUSE_116.dat",
    r"SRTCS_209751_1200467928_tdact_04954564CURACAOHOUSE_111.dat",
    r"SRTCS_221646_1200623466_poapacc_03530715_116.dat",
    r"SRTCS_221646_1200623466_tdact_03530715_111.dat",
]


def get_file_metadata(file_name: str, header: str, footer: str) -> list:
    header = header.replace("HEADER|", "").replace("DATA:", "")
    footer = footer.replace("TRAILER|", "")
    file_metadata = "|".join([header, footer])
    try:
        record = dict(item.split(": ") for item in file_metadata.split("|"))
        record["file_name"] = file_name
    except ValueError:
        print("Unexpected header/footer format verify separators")
        raise
    return [tuple(record.values())]


def check_file_integrity(file_path: str) -> list:
    """
    Check file integrity of GS files on aoi.lan location.

    Parameters
    ----------
    file : str
        File name including extension.

    Raises
    ------
    Exception
        If not all files are present, if any file misses a footer, or if the
        record count is not as expected.

    Returns
    -------
    returns a list of tuples with file metadata

    """

    file = file_path.split("/")[-1]
    if not os.path.exists(file_path):
        raise Exception(f"Missing file: {file} in {file_path}")
    with open(file_path, "r") as f:
        lines = f.read().splitlines()
        footer = lines[-1].split("|")
        if footer[0] != "TRAILER":
            msg = f"Missing footer: {file} in {file_path}. "
            msg += "File is probably incomplete, "
            msg += "verify upstream file handling."
            raise Exception(msg)
        footer_dict = {}
        for f in footer[1:]:
            k, v = f.split(": ")
            footer_dict.update({k: v})
        expected_count = int(footer_dict["Record Count"])
        record_count = len(lines) - 3
        if expected_count != record_count:
            msg = f"File length check failed: {file} in {file_path}. "
            msg += f"Counted {record_count} records but expected "
            msg += f"{expected_count} records as specified in file footer."
            raise Exception(msg)
    return get_file_metadata(file, lines[0], lines[-1])


def is_previously_moved(table_id, file_name, file_date):
    client = bigquery.Client()
    query = 'SELECT is_loaded FROM `{}` WHERE file_name = "{}" AND file_business_date = "{}" LIMIT 1'.format(table_id, file_name, file_date)
    # print(query)
    try:
        query_job = client.query(query)
        is_exist = len(list(query_job.result())) >= 1
        return is_exist
    except Exception as e:
        print(f"Error occured: {e}")
        raise
    return False


def upload_to_bucket(storage_client, bucket_name,
                     source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}. {blob.public_url}")


if __name__ == "__main__":
    if system() == "Linux":
        base_path = "/opt/BI/Incoming/GS/"
    elif system() == "Windows":
        base_path = "//aoi.lan/globalDirectory/BusinessIntelligence/Incoming/GS/"
    
    log_table_id = "ao-prototype.gs_staging.gs_file_load_log"
    parser = argparse.ArgumentParser()
    run_date = datetime.now() - timedelta(1)
    parser.add_argument("-r", "--run_date", default=run_date.strftime("%Y-%m-%d"),
                        help="enter a run date in the format YYYY-MM-dd")
    parser.add_argument("-d", "--days_ago", default=5, type=int,
                        help="how many days back should the app run (inclusive)")
    args = parser.parse_args()

    end = datetime.strptime(args.run_date, "%Y-%m-%d")
    start = end - timedelta(args.days_ago)
    date_generated = [start + timedelta(days=x)
                      for x in range(0, (end-start).days + 1)]
    storage_client = storage.Client.from_service_account_json(
        'secrets/credentials.json')
    bucket = storage_client.bucket("gs-incoming-files")
    schema = [bigquery.SchemaField("file_report_item_name", "STRING"),
              bigquery.SchemaField("file_report_item_id", "STRING"),
              bigquery.SchemaField("file_run_date", "STRING"),
              bigquery.SchemaField("file_fund_id", "STRING"),
              bigquery.SchemaField("file_advisor", "STRING"),
              bigquery.SchemaField("file_business_date", "STRING"),
              bigquery.SchemaField("file_record_count", "INTEGER"),
              bigquery.SchemaField("file_name", "INTEGER")]
    print("gcp client created")

    for report_date in date_generated:

        for file in FILES:
            file_path = base_path + report_date.strftime("%Y/%m/%d/") + file
            try:
                metadata_to_insert = check_file_integrity(file_path)
                file_business_date = metadata_to_insert[0][5]
                if not is_previously_moved(log_table_id, file,
                                           file_business_date):
                    destination_blob_name = report_date.strftime("%Y/%m/%d/") + file
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_filename(file_path)
                    bq_insert_row_into_table(log_table_id,
                                             metadata_to_insert, schema)
                    print(f"File {file} succesfully moved")
                else:
                    print(f"File {file} already loaded")

            except Exception as e:
                print(e)
