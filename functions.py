import sqlite3
import pandas as pd
from datetime import datetime

def check_header(header_row):
    # check the header row has 6 entries
    if len(header_row)!=6:
        return False

    # check the record identifier == 'HEADR'
    # check the file type identifier == 'SMRT'
    # check the company id == 'GAZ' (Gazprom)
    if not (header_row[0]=='HEADR' and header_row[1]=='SMRT'
    and header_row[2]=='GAZ'):
        return False


    # check the file creation date is in the correct format
    # YYYYMMDD (UTC)
    date_format = "%Y%m%d"
    date_string = header_row[3]
    try:
        date_valid = bool(datetime.strptime(date_string, date_format))
    except ValueError:
        return False


    # check the file creation time is in the correct format
    # HHMMSS (UTC)
    time_format = "%H%M%S"
    time_string = header_row[4]
    if len(time_string)!=6:
        return False

    try:
        time_valid = bool(datetime.strptime(time_string, time_format))
    except ValueError:
        return False

    # check the file generation number is the correct format
    # matches (PN|DV)[0-9]{6} (Production/Dev + File Number)
    generation_string = header_row[5]

    if len(generation_string)!=8:
        return False

    if not (generation_string[0:2]=='PN'
            or generation_string[0:2]=='DV'):
        return False

    for i in generation_string[2:8]:
        try:
            number = int(i)
        except ValueError:
            return False

    return True

def check_footer(footer_row):
    if footer_row[0]=='TRAIL':
        return True
    else:
        return False

def check_valid_dataset(dataset):
    if check_header(dataset.iloc[0])==True and check_footer(dataset.iloc[len(dataset[0])-1])==True:
        return True
    else:
        return False

def connect(sqlite_file):
    """ Make connection to an SQLite database file """
    conn = sqlite3.connect(sqlite_file)
    c = conn.cursor()
    return conn, c

def close(conn):
    """ Commit changes and close connection to the database """
    conn.commit()
    conn.close()

def insert_file_record(cursor,FILE_ID,DATE_RECEIVED,TIME_RECEIVED):
    cursor.execute("""INSERT INTO file_table(FILE_ID,DATE_RECEIVED,TIME_RECEIVED)
        VALUES(?,?,?)""",(FILE_ID,DATE_RECEIVED,TIME_RECEIVED))


def insert_meter_record(cursor, METER_ID, DATE_RECEIVED, TIME_RECEIVED):
    cursor.execute("""INSERT INTO meter_table(METER_ID,LAST_UPDATED_DATE,LAST_UPDATED_TIME)
        VALUES(?,?,?)""",(METER_ID,DATE_RECEIVED,TIME_RECEIVED))


def update_meter_record(cursor, METER_ID, DATE_RECEIVED, TIME_RECEIVED):
    cursor.execute(f"""UPDATE meter_table SET LAST_UPDATED_DATE={DATE_RECEIVED},
        LAST_UPDATED_TIME={TIME_RECEIVED} where METER_ID={METER_ID}""")


def insert_reading_record(cursor, LINKED_FILE, LINKED_METER, MEASUREMENT_DATE, MEASUREMENT_TIME, CONSUMPTION):
    cursor.execute("""INSERT INTO reading_table(FILE_ID,METER_ID,MEASUREMENT_DATE,MEASUREMENT_TIME,CONSUMPTION)
        VALUES(?,?,?,?,?)""",(LINKED_FILE,LINKED_METER,MEASUREMENT_DATE,MEASUREMENT_TIME,CONSUMPTION))


def search_for_value(cursor, table_name, column_name, value):
    """ return true if value is in column """
    cursor.execute(f"""SELECT {column_name} FROM {table_name} WHERE {column_name} = {value}""")
    column_data = cursor.fetchall()
    if len(column_data)>0:
        return True

    return False

def delete_from_table(cursor, table_name, column_name, value):
    """delete value from table"""
    cursor.execute(f"""DELETE FROM {table_name} WHERE {column_name}={value}""")

def file_in_database(cursor,dataset):
    if search_for_value(cursor, 'file_table', 'FILE_ID', f"'{dataset.iloc[0][5]}'")==True:
        return True

    return False

def meter_in_database(cursor,meter):
    if search_for_value(cursor, 'meter_table', 'METER_ID', f"'{meter}'")==True:
        return True
    return False

def get_file_data(dataset):
    file_id = dataset.iloc[0][5]

    now = datetime.now()
    upload_date = now.strftime("%Y%m%d")
    upload_time = now.strftime("%H%M%S")

    return file_id, upload_date, upload_time

def get_meter_data(dataset):
    meters =[]
    for i in range(1,len(dataset[0])-1):
        if dataset.iloc[i][1] not in meters:
            meters.append(dataset.iloc[i][1])

    return meters

def add_to_database(cursor, data):
    if not file_in_database(cursor,data):

        # add file_table data
        file_id, date_received, time_received = get_file_data(data)
        insert_file_record(cursor,file_id, date_received, time_received)

        # add meter_table data
        for meter in get_meter_data(data):

            # if meter number is already in database
            if meter_in_database(cursor, meter)==True:

                # change the last updated time
                now = datetime.now()
                upload_date = now.strftime("%Y%m%d")
                upload_time = now.strftime("%H%M%S")
                update_meter_record(cursor,meter,upload_date, upload_time)
            else:
                # create a new entry
                now = datetime.now()
                upload_date = now.strftime("%Y%m%d")
                upload_time = now.strftime("%H%M%S")
                insert_meter_record(cursor,meter,upload_date, upload_time)


        # add reading data to reading table
        for i in range(1,len(data[0])-1):
            linked_meter = data.iloc[i][1]
            linked_file = data.iloc[0][5]
            measurement_date = data.iloc[i][2]
            measurement_time = data.iloc[i][3]
            consumption = data.iloc[i][4]
            insert_reading_record(cursor, linked_file, linked_meter, measurement_date, measurement_time, consumption)
