import os
import shutil
import pandas as pd
import functions


"""
For all the files in the sample_data folder, go through each file and check it suits our
validity test. If true we add it to the database. If false we move the file to a
directory called invalid_data
"""


# path of sample data
path = './sample_data/'

# path of directory to move invalid data
invalid_path = './invalid_data/'


sqlite_file = "SMRTdata.db"

conn, c = functions.connect(sqlite_file)

c.execute("""CREATE TABLE IF NOT EXISTS file_table(
    FILE_ID TEXT PRIMARY KEY NOT NULL,
    DATE_RECEIVED TEXT NOT NULL,
    TIME_RECEIVED TEXT NOT NULL
)""")


c.execute("""CREATE TABLE IF NOT EXISTS meter_table(
    METER_ID TEXT PRIMARY KEY NOT NULL,
    LAST_UPDATED_DATE TEXT NOT NULL,
    LAST_UPDATED_TIME TEXT NOT NULL
)""")


c.execute("""CREATE TABLE IF NOT EXISTS reading_table(
    READING_ID INTEGER PRIMARY KEY,
    FILE_ID TEXT NOT NULL,
    METER_ID TEXT NOT NULL,
    MEASUREMENT_DATE TEXT NOT NULL,
    MEASUREMENT_TIME TEXT NOT NULL,
    CONSUMPTION TEXT NOT NULL,
    FOREIGN KEY (FILE_ID) REFERENCES file_table(FILE_ID),
    FOREIGN KEY (METER_ID) REFERENCES meter_table(METER_ID))
""")



# get files from path directory as list
files = os.listdir(path)

# for each file
for file in files:

    # check it is a file
    if os.path.isfile(path+file):

        # read to pandas df and check if it satisfies our validity tests
        data = pd.read_csv(path+file, header=None, dtype={3:str, 4:str})

        # if yes add to our database
        if functions.check_valid_dataset(data)==True:
            functions.add_to_database(c,data)
            conn.commit()
        # if not move the file into the invalid_data folder
        else:
            shutil.move(path+file,invalid_path+file)




functions.close(conn)
print("SUCCESS")
