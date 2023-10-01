import time
from google.cloud import bigquery
import logging
import json
from argus_api import list_files_in_directory


# Initialize logging
logging.basicConfig(filename='server.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def load_json(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"[Argus_Reformat] Failed to load JSON from {filename}: {e}")
        raise


def save_json(filename, data):
    try:
        with open(filename, 'w') as f:
            json.dump(data, f)
        logging.info(f"Successfully saved JSON to {filename}")
    except Exception as e:
        logging.error(f"Failed to save JSON to {filename}: {e}")
        raise


def run_job(client, table_id, job_config, file_id):
    try:
        with open(f'argus_reformated/{file_id}.csv', "rb") as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)

        while job.state != 'DONE':
            time.sleep(2)
            job.reload()

        table = client.get_table(table_id)
        logging.info(
            "[BigQuery] Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )
        return True
    except Exception as e:
        logging.error(f"[BigQuery] failed to load {file_id} with {e}")
        return False


def argus():
    # Construct a BigQuery client object
    client = bigquery.Client.from_service_account_json('configs/gcloud_api_credentials.json')

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
    )

    table_id = "energydata-400616.Forward_Curve.Argus"

    existing_files = load_json('configs/files.json')
    failed_files = existing_files.get('failed_argus_uploads')
    successful_files = existing_files.get('bigQuery_argus_files')

    file_ids = set(list_files_in_directory('argus_reformated')) - set(existing_files.get('bigQuery_argus_files'))
    logging.info(f"[BigQuery] total files to upload {len(file_ids)}")
    for file_id in list(file_ids):
        out = run_job(client, table_id, job_config, file_id)
        if out:
            successful_files.append(file_id)
        else:
            failed_files.append(file_id)

    existing_files['failed_argus_uploads'] = failed_files
    existing_files['bigQuery_argus_files'] = successful_files
    existing_files['argus_files'] = failed_files + successful_files

    save_json('configs/files.json', existing_files)


if __name__ == "__main__":
    argus()