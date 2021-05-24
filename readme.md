# README.md
### Install requirements with:
```
$ pip install -r requirements.txt
```
```
###### Requirements with Version Specifiers ######`
numpy==1.20.3
pandas==1.2.4
python-dateutil==2.8.1
pytz==2021.1
six==1.16.0
```
To run the code, download the repository and navigate to the directory.
Install the requirements with the command above and run the code with :
```
$ python main.py
```
If the code has ran correctly you should be met with an output saying "SUCCESS".

This code will go through each file in the folder 'sample_data' checking the validity of the file. If it is suitable it will be added to a Pandas DataFrame from which we use SQLite statements to export the files to a database 'SMRTdata.db' containing three tables: 'file_table', 'meter_table' and 'reading_table'. If the file is considered to be unsuitable due to incorrect header or footer format the file shall be moved from 'sample_data' to 'invalid_data'.

Note there are a number of ways this code could be improved to further meet the requirements stated in 'data_exercise.html'.  A few examples include:
- More suitable data types for variables within the database.
- Answer the questions as stated by 'data_exercise.html'.
- More sophisticated validation checks on the files when reading from 'sample_data'.




> Written with [StackEdit](https://stackedit.io/).

