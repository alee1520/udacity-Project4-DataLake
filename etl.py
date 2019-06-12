import configparser
import os
import  pyspark.sql.functions as F
from time import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.autoBroadcastJoinThreshold",-1) \
        .config("spark.sql.shuffle.partitions",500) \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process Song Data procedure
    ETL process read and load song json file 
    and extract song and artist data and save as *.parquet file
    to either project workspace or to an S3 bucket.  Also,
    add logging information to track the ETL process
    """
    
    dataSong = input_data +"/song_data/A/A/A/"
    input_data = input_data + "song_data/*/*/*"
    song_data = input_data
    
    print('======= Read song data json files to dfSong dataframe =======')
    print('Path: ' + song_data)
    print("Load schema a song file")       
    print('dfGetSampleSchema = spark.read.options(samplingRatio=0.1).json(dataSong).schema')
    
    loadTimes = []	
    t0 = time()   
    
    #Extract schema data from the file data file and populate into a variable  
    
    dfGetSampleSchema = spark.read.options(samplingRatio=0.1).json(dataSong).schema
    songschema = dfGetSampleSchema
       
    print('Load into dfSong dataframe')
    print(' dfSong = spark.read.json(song_data, schema=songschema)')
    
    dfSong = spark.read.json(song_data, schema=songschema)

    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    
    # extract columns to create songs table and drop duplicate song_id(s)
    
    loadTimes = []	
    print('======= Songs: Extract fields and drop duplicates data =======')
    print("songs_table = dfSong.select(song_id, title, artist_id, year, duration)")
    
    t0 = time()       
    songs_table = dfSong.select("song_id", "title", "artist_id", "year", "duration")
    songs_table = songs_table.dropDuplicates(["song_id"])
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))    
       
    # write songs table to parquet files partitioned by year and artist
    
    loadTimes = []	
    print('======= Songs: Create songs parquet =======')
    
    t0 = time()        
    song_parquet = output_data + "songs.parquet"
    
    print("Load song_parquet: " + song_parquet)
    print("songs_table.write.mode('overwrite').partitionBy(year,artist_id).parquet(song_parquet)")   
    
    songs_table.write.mode('overwrite').partitionBy("year","artist_id").parquet(song_parquet)
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))    
    
    # extract columns to create artists table and drop duplicate artist_id
    
    print('======= Artist: Extract fields and drop duplicates data =======')
    print("dfSong.select(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)")         
    t0 = time()       
    
    artists_table = dfSong.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    artists_table = artists_table.dropDuplicates(["artist_id"])

    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))   
    
    # create artists parquet files
    loadTimes = []	
    print('======= Artists: Create songs parquet =======')    
    
    artist_parguet = output_data + "artist.parquet"
    
    print("Load artist_parguet: " + artist_parguet)
    print("artists_table.write.mode(overwrite).parquet(artist_parguet)")   
    
    artists_table.write.mode('overwrite').parquet(artist_parguet)
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    
def process_log_data(spark, input_data, output_data):
    
    """
    Process Log Data procedure
    ETL process read and load song json file 
    and extract users, time and songplays data and save as *.parquet file
    to either project workspace or to an S3 bucket.  Also,
    add logging information to track the ETL process
    """    
    
    loadTimes = []
    print('======= Read log data json files to dfLog dataframe =======')
    
    log_data = input_data + "log_data/2018/11"    
    t0 = time() 
    
    print('Path: ' + log_data)
    print('dfLog = spark.read.json(log_data)')
    
    dfLog = spark.read.json(log_data)  
    cnt = dfLog.count()
    
    print('Total count of log data: ' + str(cnt))
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))    
    
    
    print('======= Read song data json files to dfSong dataframe =======')
    song_data = input_data + "song_data/*/*/*"
    dataSong = input_data +"/song_data/A/A/A/"
    
    print('Path: ' + song_data)
    print("Load schema a song file") 
    print("dfGetSampleSchema = spark.read.options(samplingRatio=0.1).json(dataSong).schema")    
    
    loadTimes = []    
    t0 = time() 
    
    dfGetSampleSchema = spark.read.options(samplingRatio=0.1).json(dataSong).schema
    songschema = dfGetSampleSchema        
    
    print('dfSong = spark.read.json(song_data, schema=songschema) ')
    dfSong = spark.read.json(song_data, schema=songschema)   
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    

    # extract columns for users data and drop duplicate userId   

    print('======= Users: Extract fields and drop duplicates data =======')
    print('dfLog.select("userId","firstName", "lastName", "gender", "level")')
    print('')
    loadTimes = []
    t0 = time()
    
    users_table = dfLog.select("userId","firstName", "lastName", "gender", "level")
    users_table = users_table.dropDuplicates(['userId'])
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
    
    # create users parquet file(s)
    
    print('======= Users: Create users parquet =======')
    print('users_table.write.mode(overwrite).parquet(users_parguet)')
    loadTimes = []
    t0 = time()
    
    users_parguet = output_data + "users.parquet"    
    users_table.write.mode('overwrite').parquet(users_parguet)
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))    
        
    # create timestamp/datetime column and extract columns from original timestamp column

    print('======= Time: Create Time table from ts column and drop duplicates data =======')
    print('time_table.withColumn(datetime, from_unixtime((time_table.ts/1000) .........')
    
    loadTimes = []
    t0 = time()
    
    time_table = dfLog.select("ts")
    time_table = time_table.withColumn('datetime', from_unixtime((time_table.ts/1000),'yyyy-MM-dd HH:mm:ss.SSSS')) .\
                    withColumn('hour', hour('datetime')) .\
                    withColumn('day', dayofmonth('datetime')) .\
                    withColumn('week', weekofyear('datetime')) .\
                    withColumn('month', month('datetime')) .\
                    withColumn('year', year('datetime')) .\
                    withColumn('weekday', dayofweek('datetime')) .\
                    withColumnRenamed('ts','milliseconds') .\
                    withColumn('datetime', F.to_timestamp('datetime'))   
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))        
    
    # Create Time parquet and partition by year and month

    print('======= Time: Create time parquet =======')
    print('time_table.write.mode(overwrite).partitionBy("year","month").parquet(time_parquet)')
    
    loadTimes = []
    t0 = time()    
    
    time_parquet = output_data + "time.parquet"   
    time_table.write.mode('overwrite').partitionBy("year","month").parquet(time_parquet)

    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime))     
    
    # extract columns from song and log json to create songplays 
    
    print('======= SongPlays: Create SongPlays Time =======')
    print('Join logfiles and songfiles data to create the SongPlays dataset')
    print('Create temp views to be used in sql statment to load songplays dataframe')
    
    loadTimes = []
    t0 = time()  
    
    #Create temp views to be used in songplays query needed to created parquet file
    
    print('>>> dfLog.createOrReplaceTempView(Log) ....')         
    dfLog.createOrReplaceTempView("Log")
    
    print('>>> dfSong.createOrReplaceTempView(Songs) ....')
    dfSong.createOrReplaceTempView("Songs")
    
    print('>>> dfTimeTable.createOrReplaceTempView("Time") .....')
    time_table.createOrReplaceTempView("Time")
    
    print('Load dataframe songplays_table based on sql statement ')
    print('spark.sql(select t.year, t.month, datetime start_time,......)')
    
    #Use spark sql to create the necessary dataset to load songplays table/parquet
    songplays_table =spark.sql("""
                            select  
                                   t.year, t.month, datetime start_time, 
                                   userid, level, s.song_id, s.artist_id, 
                                   sessionId, location, userAgent
                                from Log l 
                                inner join Time t
                                    on l.ts = t.milliseconds
                                left join Songs s
                                    on  s.artist_name = l.artist
                                    and s.title = l.song
                                    and s.duration = l.length
                             """)
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime)) 
    
    #print("Remove duplicates")
    #print("songplays_table = songplays_table.dropDuplicates(['userid','level','song_id','artist_id','sessionId'])")
    #songplays_table = songplays_table.dropDuplicates(['userid','level','song_id','artist_id','sessionId'])
    
    print("Add unique index id name called songplays_id")
    print("songplays_table.withColumn('songplays_id',monotonically_increasing_id() +1)")
    
    songplays_table = songplays_table.withColumn("songplays_id",monotonically_increasing_id() +1)
    
    # write songplays table to parquet files partitioned by year and month

    print('======= SongPlays: Create SongPlays parquet =======')
    print('songplays_table.write.mode(overwrite).partitionBy("year","month").parquet(songplays_parquet)')
    
    loadTimes = []
    t0 = time()      
    
    songplays_parquet = output_data + "songplays.parquet"
    songplays_table.write.mode('overwrite').partitionBy("year","month").parquet(songplays_parquet)
    
    loadTime = time()-t0          
    loadTimes.append(loadTime)
    print("=== DONE IN: {0:.2f} sec\n".format(loadTime)) 

def main():
    print(">>>>>>>>>>> Start Sarkify etl.py log file <<<<<<<<<<<<<")
    
    starttimestamp = datetime.now() 
    start = time()
    
    print("Start Time: " + str(starttimestamp))
    
    spark = create_spark_session()
    print('Establish spark session')
    
    input_data = "s3a://udacity-dend/"
    output_data = "./files/spark-warehouse/"

    
    print('Begin: Procedure process_song_data')
    
    start_psd = time()
    process_song_data(spark, input_data, output_data) 
    end_psd = time()
    totalrun = (end_psd - start_psd) /60
    totalrun = round(totalrun)
    
    print('Total Run time: ' + str(totalrun))
    print('End: Procedure process_song_data ')
    
    print('Begin: Procedure process_log_data')
    
    start_pld = time()
    process_log_data(spark, input_data, output_data)
    end_pld = time()
    totalrun = (end_pld - start_pld) /60
    totalrun = round(totalrun)
    
    print('Total Run time: ' + str(totalrun))
    print('End: Procedure process_log_data')
    print('')
    
    endtimestamp =datetime.now() 
    end = time()      
    totalrun = end - start
    totalrun_min = round(totalrun/60)
    totalrun_hr = round((totalrun/60)/60)
    
    print("End Time: " + str(endtimestamp ))
    print("Total Run Time by second(s): = " + str(totalrun))      
    print("Total Run Time by minute(s): = " + str(totalrun_min)) 
    print("Total Run Time by hour(s): = " + str(totalrun_hr))      
    print(">>>>>>>>>>> End etl.py log file <<<<<<<<<<<<<")      

if __name__ == "__main__":
    main()
