import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
from argus_api import list_files_in_directory

# Initialize logging
logging.basicConfig(filename='server.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')


def load_config():
    try:
        with open("configs/config.json", "r") as f:
            return json.load(f).get('argus_reformat')
    except Exception as e:
        logging.error(f"[Argus_Reformat] Failed to load config: {e}")
        raise


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
    except Exception as e:
        logging.error(f"[Argus_Reformat] Failed to save JSON to {filename}: {e}")
        raise


def delete_file_by_id(file_id, directory_path):
    try:
        file_path = os.path.join(directory_path, file_id)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logging.info(f"[Argus_Reformat] Successfully deleted file: {file_id}")
        else:
            logging.warning(f"[Argus_Reformat] File {file_id} not found in directory: {directory_path}")

    except FileNotFoundError:
        logging.error(f"[Argus_Reformat] File {file_id} not found.")
    except PermissionError:
        logging.error(f"[Argus_Reformat] Permission denied to delete file {file_id}.")
    except Exception as e:
        logging.error(f"[Argus_Reformat] An error occurred while deleting file {file_id}: {e}")


def read_excel(file_path, sheet):
    try:
        return pd.read_excel(file_path, header=None, sheet_name=sheet)
    except Exception as e:
        logging.error(f"[Argus_Reformat] Failed to read Excel file {file_path} sheet {sheet}: {e}")
        return pd.DataFrame()


def clean_dataframe(df):
    df = df.iloc[:98, :]  # Delete rows after row 98
    try:
        df = df[df[2] != "Calendar Blocks"]
    except:
        pass

    df = df[df[
                2] != "Monthly Curves"]  # Delete the row containing "Monthly Curves" in column C (2 in zero-based index)
    df.drop(index=range(0, 7), inplace=True)  # Delete rows 1-7
    df.reset_index(drop=True, inplace=True)  # Reset the index  # Delete columns A and B
    df.drop(columns=[0, 1], inplace=True)
    return df


def transform(df, close_of_business_day):
    data = []
    # Iterate through the DataFrame to create the new structure
    for col in range(3,
                     len(df.columns)):  # Start from the 3rd column, as it contains the first set of price data
        market = df.iloc[0, col]
        location = df.iloc[1, col]
        peak = df.iloc[2, col]
        curve = df.iloc[3, col]

        if pd.isna(curve):
            curve = "Mid"

        for row in range(3, len(df)):  # Start from the 3rd row, as it contains the first set of month data
            month = df.iloc[row, 0]
            price = df.iloc[row, col]

            if pd.notna(month) and pd.notna(price):
                # Check if both the month and price are non-null
                try:

                    try:
                        created = close_of_business_day.date()
                    except:
                        start_date = datetime(1899, 12, 30)
                        converted_date = start_date + timedelta(days=int(close_of_business_day))
                        created = converted_date.date()

                    try:
                        month = month.date()
                    except:
                        try:
                            month = datetime.strptime(month, "%b-%Y").date()
                        except:
                            start_date = datetime(1899, 12, 30)
                            converted_date = start_date + timedelta(days=int(month))
                            month = converted_date.date()

                    new_row = {
                        'Created': created,
                        'Month': month,
                        'Market': market,
                        'Location/Zone': location,
                        'On Peak/ Off Peak/ RTC': peak,
                        'Mid/ Bid/ Offer': curve,
                        'Price': float(price)
                    }
                    data.append(new_row)
                except:
                    pass

    return data


def reformat_file(fileID):
    file_path = f'argus_downloads/{fileID}.xlsx'
    logging.info(f"[Argus_Reformat] Processing {fileID}")

    data = []
    sheets = [0, 1]  # Add more sheets as needed
    for sheet in sheets:
        df = read_excel(file_path, sheet)
        if df.empty:
            sheets.append(len(sheets))
            logging.warning(f"[Argus_Reformat] Sheet {sheet} in file {file_path} is empty.")
            continue

        close_of_business_day = df.iloc[5, 3]
        df_cleaned = clean_dataframe(df)
        data += transform(df_cleaned, close_of_business_day)
        # Convert the list of dictionaries to a DataFrame

    csv_df = pd.DataFrame(data)
    logging.info(f"[Argus_Reformat] Successfully processed file: {fileID}")
    # Save the final corrected DataFrame to a CSV file
    csv_file_path = f'argus_reformated/{fileID}.csv'
    csv_df.to_csv(csv_file_path, index=False)


def main():
    config = load_config()
    USE_FILES_JSON = config.get('use_files_json')

    fileIDs = list_files_in_directory("argus_downloads")
    processed = []
    existing_files = load_json('configs/files.json')

    if USE_FILES_JSON:
        old_fileIDs = existing_files['argus_files']
    else:
        old_fileIDs = set(list_files_in_directory('argus_reformated'))

    for fileID in fileIDs:
        if fileID not in old_fileIDs:
            try:
                reformat_file(str(fileID))
                processed.append(fileID)
            except:
                logging.error(f"[Argus_Reformat] Error processing {fileID}")

    logging.info(f"[Argus_Reformat] Files processed: {len(processed)}")
    existing_files['argus_files'] += processed
    save_json('configs/files.json', existing_files)


if __name__ == "__main__":
    main()
