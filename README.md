Spark Sparkify:

The purpose of the Sparkify Database is to gather and maintain activity on the sparkify app from the active users and provide analytics to gauge the use of the app.

Tech Spec:

There are two data files that sparkify extract data from for analytics purpose.  Song data and Log data.  The data are stored in s3 in a json format.  The data from the json files are extracted with python and transformed (manipulated) and loaded (ETL) and save as separate parquet files based on the topic/subject matter of the data.

Execution:
    Steps for executing the ETL process to load data
    1.	Open Jupyter Notebooks in your workspace
    2.	Modify dl.cfg with the necessary AWS credentials 
    3.	Open etl.py and if necessary modify the variable “output_data” directory path
    4.	Launch a new notebook tab 
    5.	Run the following command	
    a.	%run etl.py
    
The etl.py file will execute the ETL process to move the data from s3 to the output directory specified.  It will create the parquet files based on the different subject matter.

Run Sample Report

The sample report provide simply analytical report based on the data that was process.  This can also provide verification that the data is available for the user.

Step for executing the Sample Report:
    1.	Open Jupyter Notebook in your workspace
    2.	Open sample_report.py and modify the “filepath” variable to point to the directory of your parquet file(s)
    3.	Launch a new notebook tab
    4.	Run the following command
    a.	%run sample_report.py
    5.	The report will display simple graphs based on sample queries within the python program
