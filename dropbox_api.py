from os import mkdir
from dropbox import Dropbox
from os.path import exists, join, curdir
from time import sleep
import logging
from datetime import datetime
import csv

# Not in POC I would change the token from a local variable to an environment variable and read it.
token = 'sl.AxRRhh_N4QODLoNJD8ghoSvKkrL-oLtE9GfvU1KewUsMhfRzQTTbOshufwBV48onxeWDDVOVFmi9Ma_LaN9RvfuBNadFPCJajvLEZFbFWM7niJgYiXH-gmVJlMkIxpBrMTKsIzM'

# Dictionary to be converted into csv database in which we will store the csv files that we have already handled.
csv_database = {
    'file_name': [],
    'is_valid': [],
}

# Logger for runtime info.
log = logging.getLogger(__name__)

"""
Architecture:
- Check if database.csv file exists -> read all the files that were already handled and create a dictionary.
- Create polling loop which is responsible for checking the shared folder every X seconds (for POC -> X = 5)
- Check if a new csv file was uploaded to the shared folder.
    - If the CSV as wrong name (Wrong format) - Add it to csv database (optional expansion: notify client for mistake)
- Parse the file name to: {Name, DD-MM-YYYY, HHMMSS}
- Download the CSV file into the destined directory and insert it into csv in order to follow the downloaded files.
    - If a new CSV file was added to the database, update the csv file.
    
- Special scenarios:
    1. If the CSV as wrong name - Add it to csv database as invalid..
    2. Check only CSV files.
    
- Pay attention to:
    - Valid permissions.
    - Valid token in order to connect the app to the dropbox

- Storing the data in local directory:
    - Format: \\Clients\\PC_Name\\Date\\Time
    - Example: 
        - Filename = PC3_20_10_2020_102050.csv
        - The data will be stored in: \\Clients\\PC3\\20-10-2020\\102050.csv
"""


def parse_file_name(filename: str):
    """
    This function is responsible for parsing file name from the following format:
    - Format: Name_DD_MM_YY_HHMMSS
        - If the csv is in the wrong format - add it to the csv database as invalid.
    :param filename: given filename to parse.
    :return: Name, DD-MM-YYYY, HHMMSS(new filename)
    """
    details = filename.split("_")
    if len(details) == 5:
        date = "{}-{}-{}".format(details[1], details[2], details[3])
        return details[0], date, details[4]
    else:
        log.error("{}: Invalid file name format: {}".format(datetime.now(), filename))

        csv_database['file_name'].append(filename)
        csv_database['is_valid'].append('invalid')

        raise ValueError(filename)


def handler(dbx: Dropbox, filename: str):
    """
    This function is responsible for:
    - Creating the Clients directory (if doesn't exist)
    - For each client, create it's own directory for his files using his machine name (if doesn't exist)
    - In each client directory, csv files are arranged into directories by dates. (which are created if don't exist)
    - Finally, downloading the csv file into the destined directory and inserts it into csv file in order to follow
      the downloaded files.
    :return: None
    """
    # define file properties
    uploads_filename = '/Uploads/' + filename
    client_name, date, time = parse_file_name(filename=filename)

    # define paths
    clients_directory = join(curdir, 'Clients')
    client_path = join(clients_directory, client_name)
    date_path = join(client_path, date)

    # check dir existance
    if not exists(clients_directory):
        mkdir(clients_directory)
        log.info("{}: Clients directory was created".format(datetime.now()))

    if not exists(client_path):
        mkdir(client_path)
        log.info("{}: {} directory was created".format(datetime.now(), client_path))

    if not exists(date_path):
        mkdir(date_path)
        log.info("{}: {} directory was created".format(datetime.now(), date_path))

    # download new file into path as time.csv
    dbx.files_download_to_file(join(date_path, time), uploads_filename)
    log.info("{}: File {} was downloaded successfully to {}".format(datetime.now(), time, date_path))

    # Once the file was downloaded, insert it into csv file which is responsible for downloaded files.
    csv_database['file_name'].append(filename)
    csv_database['is_valid'].append('valid')


def write_to_csv(filename: str):
    """
    This function is responsible for writing our csv database (dictionary) into csv file.
    :param filename: A file name to write the dictionary to.
    :return: None
    """
    with open(filename, mode='w') as db_csv:
        fieldnames = ['File Name', 'Validity']
        writer = csv.DictWriter(db_csv, delimiter=',', fieldnames=fieldnames, lineterminator='\n')
        writer.writeheader()
        for i in range(len(csv_database['file_name'])):
            writer.writerow({'File Name': csv_database['file_name'][i], 'Validity': csv_database['is_valid'][i]})


def read_from_csv(filename: str):
    """
    This function is responsible for reading our csv database from csv file into dictionary.
        - If the file doesn't exist (first iteration) raise FileNotFoundError and return.
    :param filename: A file name to read the csv from.
    :return: None
    """
    try:
        my_csv = csv.DictReader(open(filename))
        for row in my_csv:
            csv_database['file_name'].append(row['File Name'])
            csv_database['is_valid'].append(row['Validity'])
    except FileNotFoundError:
        return


def main():
    """
    This function is responsible for executing the script.
    - Define Logger & Dropbox connection.
    - update_csv (flag) - an indicator for updating our csv database file if a new file was added.
    - If an update occurred or the user interrupted the program - write the csv database (dictionary) into csv file.
    :return:
    """
    logging.basicConfig(filename='runtime.log')
    logging.getLogger().setLevel(logging.INFO)
    dbx = Dropbox(token)
    file_name_csv = 'database.csv'
    read_from_csv(filename=file_name_csv)
    try:
        while True:
            try:
                update_csv = False
                files = dbx.files_list_folder(path='/Uploads')
                for f in files.entries:
                    if f.name.endswith('.csv') and f.name not in csv_database['file_name']:
                        handler(dbx=dbx, filename=f.name)
                        update_csv = True
                if update_csv:
                    write_to_csv(filename=file_name_csv)
                    log.info("{}: {} was created.".format(datetime.now(), file_name_csv))
                sleep(5)
            except ValueError as e:
                print("Invalid file name: " + str(e))
    except KeyboardInterrupt:
        print('Exit by user')
    finally:
        write_to_csv(filename=file_name_csv)


if __name__ == '__main__':
    main()
