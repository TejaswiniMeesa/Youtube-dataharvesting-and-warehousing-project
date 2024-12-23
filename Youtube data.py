import streamlit as st
import mysql.connector
from mysql.connector import Error
import pandas as pd
import googleapiclient.discovery
from googleapiclient.errors import HttpError
api_service_name = "youtube"
api_version = "v3"
api_key = "YOUR API KEY"
youtube = googleapiclient.discovery.build(api_service_name, api_version,developerKey=api_key)
#creating connection to sql
def connect_db():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="youtube_data"
    )
mydb = connect_db()
cursor = mydb.cursor() 
#getting channel information
def get_channel_data(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    Channel_data = {
            "channel_id":response['items'][0]['id'],
            "channel_name":response['items'][0]['snippet']['title'],   
            "channel_desc":response['items'][0]['snippet']['description'],
            "channel_sub":response['items'][0]['statistics']['subscriberCount'],
            "channel_views":response['items'][0]['statistics']['viewCount'],
            "playlist_id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],   
            }
    return Channel_data
#getting video ids
def get_video_ids(channel_id):
    video_ids = []
    try:
        request = youtube.channels().list(
                  part="contentDetails",
                  id=channel_id
        )
        response = request.execute()
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = None
        while True:
            request1 = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                pageToken=next_page_token
            )
            response1 = request1.execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token = response1.get('nextPageToken')
            if not next_page_token:
                break
    except Exception as e:
        print(f"Error fetching video IDs for channel {channel_id}: {e}")
    return video_ids
#getting video information           
def videos_info(video_ids):
    videos_data=[]
    for video_id in video_ids:
        
        request2 = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id,
            maxResults=25  
        )
        response2 = request2.execute()
        def time_duration(t):
            a = pd.Timedelta(t)
            b=a.total_seconds()
            return b
        for item in response2['items']:
            data1={"channel_id":item['snippet']['channelId'],
                "channel_Name":item['snippet']['channelTitle'],
                "video_id":item['id'],
                "video_title":item['snippet']['title'],
                "video_desc":item['snippet']['description'],
                "video_pat":item['snippet']['publishedAt'],
                "thumbnails":item['snippet']['thumbnails']['default']['url'],
                "duration":time_duration(item['contentDetails']['duration']),
                "caption_status":item['contentDetails']['caption'],
                "viewCount":item['statistics'].get('viewCount',0),
                "likeCount":item['statistics'].get('likeCount',0),
                "fav_Count":item['statistics'].get('favoriteCount',0),
                "commentCount":item['statistics'].get('commentCount',0)
                }
            videos_data.append(data1)                  
    return videos_data
#getting comment information
def comment_info(video_ids):           
    Comment_data=[]
    try:
            for video_id in video_ids:
                request3 = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults=100                           
                )
                response3 = request3.execute()
                for item in response3['items']:
                    data2={"comment_id":item['snippet']['topLevelComment']['id'],
                           "video_id":item['snippet']['topLevelComment']['snippet']['videoId'],
                            "comment_text":item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "comment_author":item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "comment_pat":item['snippet']['topLevelComment']['snippet']['publishedAt']
                        }
                    Comment_data.append(data2) 
    except:
        pass
    return Comment_data
#creating tables
def create_tables():
        mydb = connect_db()
        cursor = mydb.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                       Channel_ID VARCHAR(255) PRIMARY KEY,
                       Channel_Name VARCHAR(255),
                       Channel_description TEXT,
                       Channel_Subcribers_count int,
                       Channel_views INT,
                       playlist_id VARCHAR(100))""")
        cursor.execute("""
             CREATE TABLE IF NOT EXISTS videos ( 
                       Video_Id VARCHAR(255) PRIMARY KEY,
                       Channel_Name VARCHAR(255),
                       Title VARCHAR(255),
                       Description TEXT,
                       Published_at DATETIME,
                       Thumbnails  VARCHAR(255),
                       Duration   INT,
                       Caption_status VARCHAR(255),
                       View_Count INT,
                       Like_Count INT,
                       Favorite_Count INT,
                       Comment_Count  INT,
                       Channel_ID VARCHAR(100),
                       FOREIGN KEY (Channel_ID) REFERENCES channels(Channel_ID))""")
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                       Comment_Id VARCHAR(255) PRIMARY KEY,
                        text TEXT,
                       author VARCHAR(255),
                       published_at DATETIME,
                       Video_Id VARCHAR(255),
                       FOREIGN KEY (Video_Id) REFERENCES videos(Video_Id))""")
        print("Tables created or already exist.")
        cursor.close()
create_tables()
#inserting data into channels table
def store_channel_data(mydb,channel_details):
    try:
        cursor = mydb.cursor()
        cursor.execute("use youtube_data")
        query = """
            INSERT INTO channels (Channel_ID, Channel_Name, Channel_description, Channel_Subcribers_count, Channel_views,playlist_id)
            VALUES (%s, %s, %s, %s, %s,%s)
           """
        cursor.execute(query, (channel_details['channel_id'], channel_details['channel_name'], channel_details['channel_desc'], 
                               channel_details['channel_sub'], channel_details['channel_views'], channel_details['playlist_id'], ))
        mydb.commit()
        cursor.close()
        print("Channel data inserted successfully")
    except Error as e:
        print(f"Error: {e}")
#inserting data into videos table
from datetime import datetime
def insert_video_data(channel_id):
    video_ids=get_video_ids(channel_id)
    videos_data=videos_info(video_ids)
    return videos_data
def store_video_data(mydb,videos_data,Channel_ID):
    try:
        cursor = mydb.cursor()
        cursor.execute("use youtube_data")
        query = """
            INSERT INTO videos (Video_Id,Channel_Name,Title,Description,Published_at,Thumbnails,Duration,Caption_status,
            View_Count,Like_Count,Favorite_Count,Comment_Count,Channel_ID)
            VALUES (%s, %s, %s, %s, %s,%s,%s, %s, %s, %s, %s,%s,%s)"""
        for video in videos_data:
                video_datetime_str=video['video_pat']
                video_datetime=datetime.strptime(video_datetime_str,"%Y-%m-%dT%H:%M:%SZ")
                cursor.execute(query,(video['video_id'],video['channel_Name'],video['video_title'],video['video_desc'],video_datetime,
                                        video['thumbnails'],video['duration'],video['caption_status'],video['viewCount'],
                                        video['likeCount'],video['fav_Count'],video['commentCount'],Channel_ID))
        mydb.commit()
        cursor.close()
        print("Video data inserted successfully")
    except Error as e:
        print(f"Error: {e}")
#inserting data into comment table
from datetime import datetime
def insert_comment_data(channel_id):
    video_ids=get_video_ids(channel_id)
    Comment_data = comment_info(video_ids)
    return Comment_data
def store_comment_data(mydb,Comment_data):
        try:
            cursor = mydb.cursor()
            cursor.execute("use youtube_data")
            query = """
                INSERT INTO comments (Comment_Id,text,author,published_at,Video_Id)
                VALUES(%s, %s, %s, %s,%s)
                ON DUPLICATE KEY UPDATE
                text = VALUES(text),
                author = VALUES(author),
                published_at = VALUES(published_at)"""
            for comment in Comment_data:
                comment_datetime_str=comment['comment_pat']
                comment_datetime=datetime.strptime(comment_datetime_str,"%Y-%m-%dT%H:%M:%SZ")
                cursor.execute(query, (comment['comment_id'], comment['comment_text'], comment['comment_author'],comment_datetime,comment['video_id']))
            mydb.commit()
            cursor.close()
            print("comment data inserted successfully")
        except Error as e:
            print(f"Error: {e}")
# sql queries
def execute_query(query, mydb):
    cursor = mydb.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result
query_list={
        "1.What are the names of all the videos and their corresponding channels?":
        """SELECT v.Title, c.Channel_Name
        FROM videos v
        INNER JOIN channels c ON v.Channel_ID = c.Channel_ID""",
        "2.Which channels have the most number of videos, and how many videos do they have?":
        """SELECT c.Channel_Name, COUNT(v.Video_ID) AS Num_Videos
          FROM channels c INNER JOIN videos v ON c.Channel_ID = v.Channel_ID
            GROUP BY Channel_Name ORDER BY Num_Videos DESC LIMIT 1""",
        "3.What are the top 10 most viewed videos and their respective channels?":
        """SELECT v.Title, c.Channel_Name, v.View_Count
        FROM videos v
        INNER JOIN channels c ON v.Channel_ID = c.Channel_ID
        ORDER BY v.View_Count DESC
        LIMIT 10""",
        "4.How many comments were made on each video, and what are their corresponding video names?":
        """SELECT v.Title,v.Comment_Count
        FROM videos v
        ORDER BY Comment_Count DESC""",
        "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        """SELECT v.Title, c.Channel_Name, v.Like_Count
        FROM videos v
        INNER JOIN channels c ON v.Channel_ID = c.Channel_ID
        ORDER BY v.Like_Count DESC""",
        "6.What is the total number of likes for each video, and what are their corresponding video names?":
        """SELECT v.Title, v.Like_Count from videos v
        order by v.Like_Count DESC""",
        "7.What is the total number of views for each channel, and what are their corresponding channel names?":
        """SELECT Channel_Name, Channel_views AS Total_Views
        FROM channels
        ORDER BY Channel_views DESC""",
        "8.What are the names of all the channels that have published videos in the year 2022?":
        """SELECT DISTINCT c.Channel_Name
        FROM channels c
        INNER JOIN videos v ON c.Channel_ID = v.Channel_ID
        WHERE YEAR(v.Published_at) = 2022""",
        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        """SELECT c.Channel_Name, AVG(v.Duration) AS Average_Duration
        FROM channels c
        INNER JOIN videos v ON c.Channel_ID = v.Channel_ID
        GROUP BY c.Channel_Name """,
        "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        """SELECT v.Title, c.Channel_Name, v.Comment_Count
        FROM videos v
        INNER JOIN channels c ON v.Channel_ID = c.Channel_ID
        ORDER BY v.Comment_Count DESC"""
    }
#code for streamlit app
def main():
    st.sidebar.title(":red[Youtube Data]")
    selected=st.sidebar.selectbox("select Page",["Home","Data Harvesting","Data Warehousing","Data Analysis"])
    if selected=="Home":
        st.title("Youtube Data Harvesting and Warehousing using SQL and Streamlit")
        st.markdown("""<p style='font-size: 20px;'>The project involves creating a streamlit app for users to access and analyse youtube channel information.
                     the data is extracted using youtube data api key and inserted into sql database</p>""", unsafe_allow_html=True)
        st.subheader(":red[Skills take away:]")
        st.markdown("<p style='font-size: 20px;'>Python scripting, Data Collection, Streamlit, API integration, Data Management using SQL</p>",unsafe_allow_html=True)
        st.subheader(":red[Developed by:]")
        st.markdown("<p style='font-size: 20px;'>Meesa Tejaswini</p>",unsafe_allow_html=True)
    elif selected=="Data Harvesting":
        st.header("Complete Channel information",divider="red")
        channel_id=st.text_input("Enter the channel id:")
        if st.button("Fetch channel data"):
                channel_details = get_channel_data(channel_id)
                st.write(channel_details)
        if st.button("Fetch Video Data"):
                video_ids = get_video_ids(channel_id)
                video_details = videos_info(video_ids)
                st.write(video_details)
        if st.button("Fetch Comment Data"):
                video_ids = get_video_ids(channel_id)
                Comment_details = comment_info(video_ids)
                st.write(Comment_details)
    elif selected=="Data Warehousing":
        st.header("Migrating to SQL",divider="red")
        channel_id=st.text_input("Enter the channel id:")
        if st.button("store data"):
            channel_details = get_channel_data(channel_id)
            store_channel_data(mydb,channel_details)
            st.success("channel data fetched and stored successfully!")
            videos_data=insert_video_data(channel_id)       
            store_video_data(mydb,videos_data,channel_id)
            st.success("video data fetched and stored successfully!")
            Comment_data = insert_comment_data(channel_id)       
            store_comment_data(mydb,Comment_data)
            st.success("comment data fetched and stored successfully!")
    elif selected=="Data Analysis":
        st.header("Queries",divider="red")
        query_question = st.selectbox("Select Query", [
                "1.What are the names of all the videos and their corresponding channels?",
                "2.Which channels have the most number of videos, and how many videos do they have?",
                "3.What are the top 10 most viewed videos and their respective channels?",
                "4.How many comments were made on each video, and what are their corresponding video names?",
                "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6.What is the total number of likes for each video, and what are their corresponding video names?",
                "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                "8.What are the names of all the channels that have published videos in the year 2022?",
                "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10.Which videos have the highest number of comments, and what are their corresponding channel names?"])
        if st.button("Execute"):
                query = query_list[query_question]
                result=execute_query(query, mydb)
                st.write("Query Results:")
                for row in result:
                    st.write(row)		
if __name__ == "__main__":
    main()
