import configparser
import os
import  pyspark.sql.functions as F
import matplotlib.pyplot as plt
from time import time
from datetime import datetime
from pyspark.sql import SparkSession
from IPython import get_ipython

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

def generate_sample_rpt(spark,path):
    
    """
    #### Sample DashBoards ####

    Load sample queries needed to create the sample dashboards
    The database credentials are load from dl.cfg congfile
    The queries are populated in different variables and loaded into different dataframes
    """   
    
    print('#### Sample DashBoards ####')
    print('Load sample queries needed to create the sample dashboards ......')

    #Get the path of the parquet files
    artistpath = path + '/artist.parquet'
    songspath = path + '/songs.parquet'
    userspath = path + '/users.parquet'
    timepath = path + '/time.parquet'
    songplayspath = path + '/songplays.parquet'
    
    print('Read and load parquet files .....')

    #read parquet files 
    dfArtists = spark.read.parquet(artistpath)
    dfSongs = spark.read.parquet(songspath)
    dfUsers = spark.read.parquet(userspath)
    dfTime = spark.read.parquet(timepath)
    dfSongPlays = spark.read.parquet(songplayspath)

    #Create temp view to query the parquet files

    print('Load parquet files into temp tables to be queried....')
    
    dfArtists.createOrReplaceTempView('artists')
    dfSongs.createOrReplaceTempView('songs')
    dfUsers.createOrReplaceTempView('users')
    dfTime.createOrReplaceTempView('time')
    dfSongPlays.createOrReplaceTempView('songplays')


    #Create sample analytic queries to run

    print('Create and populate sample queries ....')
    
    qryTotalSongs=spark.sql("""
    select * from (
    select count(*) total, title from songplays sp 
        inner join songs s
            on sp.song_id = s.song_id
    group by  title) x
    where total >= 5 
    order by total
    """)

    qryTotalUsersByGender = spark.sql("""
    select count(*) total, gender from users
    where gender is not null
    group by gender
    """)

    qryTotalByLevel = spark.sql("""
    select count(*) total, level from users
    where userId is not null
    group by level
    """)

    qryLevelByGender=spark.sql("""
    select count(*) total,
        case when gender ='M' then 'Male'  || ' - ' || level
             when gender ='F' then 'Female' || ' - ' || level
        end as genderbylevel    
    from users
    where gender is not null
    group by gender, level
    """)

    stats_song = qryTotalSongs.toPandas()
    stats_userbygender =  qryTotalUsersByGender.toPandas()
    stats_totalbylevel =  qryTotalByLevel.toPandas()
    stats_levelbygender = qryLevelByGender.toPandas()
    
    """
    #### Sample DashBoards ####
    Builds the dashboards based on the queries 
    """

    print('Create sample dashboards .....')
    
    # Displays graph outside the script
    get_ipython().run_line_magic('matplotlib', 'inline')
    
    # Most Song Played

    plt.bar(stats_song.total, stats_song.title)
    plt.title('Most Song Played')
    plt.ylabel('Songs')
    plt.xlabel('Number Of Plays')
    plt.show()

    # Total Users by Gender
    plt.barh(stats_userbygender.gender, stats_userbygender.total)
    plt.title('Total Users by Gender')
    plt.ylabel('Gender')
    plt.xlabel('Total')
    plt.show()

    # Total by levels
    fig1, ax1 = plt.subplots()
    ax1.pie(stats_totalbylevel.total, labels=stats_totalbylevel.level, explode=None,  autopct='%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')
    plt.show()

    # Total level by Gender 
    fig1, ax1 = plt.subplots()
    ax1.pie(stats_levelbygender.total, labels=stats_levelbygender.genderbylevel, explode=None,  autopct= '%1.1f%%',
            shadow=True, startangle=90)
    ax1.axis('equal')
    plt.show()

def main():
    
    #Filepath would need to be same as where the parquets files are saved
    
    filepath = "./files/spark-warehouse/"
    spark = create_spark_session()
    generate_sample_rpt(spark,filepath)
    
if __name__ == "__main__":
    main()
    