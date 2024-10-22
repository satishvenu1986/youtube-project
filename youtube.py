import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pymysql as sql
import pandas as pd
import re
from sqlalchemy import create_engine

api_key = 'AIzaSyCcip4n3aXS7zoNONXgl_CIggRn1dwyRJA'
youtube=build("youtube", "v3", developerKey=api_key)

# guvi - UCduIoIMfD8tT3KoU0-zBRgQ
# Learn electrinics india - UCoZYdvQauxVr4QhsbL-hCpQ
# Sans innovations - UCDwCrWeGnSGjY4K6pidLpjw


def channel_request(channel_id):
  
  response = youtube.channels().list(id=channel_id,part='snippet,statistics,contentDetails')
  channel_data = response.execute()
  channel={ 'Channel_Name':channel_data['items'][0]['snippet']['title'],
                'Channel_ID':channel_data['items'][0]['id'],
                'Channel_Subscription':channel_data['items'][0]['statistics']['subscriberCount'],
                'Channel_View':channel_data['items'][0]['statistics']['viewCount'],
                'Channel_Video':channel_data['items'][0]['statistics']['videoCount'],
                #'Channel_Description': channel_data["items"][0]["snippet"]["description"],
                'Channel_Playlistid': channel_data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]}
  return channel

def channel_mysql_write(channel_data):
    channel=pd.DataFrame(channel_data,index=[0])
    #st.write(channel)
    conn=sql.connect(host='localhost',user='root',password='Sans2024')
    cur=conn.cursor()
    cur.execute("create database if not exists demo3")
    cur.execute('use demo3')
    cur.execute('''create table if not exists Channels(Channel_Name VARCHAR(100),Channel_ID VARCHAR(100),
                                                                Channel_Subscription BIGINT,Channel_View BIGINT, Channel_Video BIGINT,Channel_playlistid VARCHAR(100) )''')
    conn.commit()
    connection_str = "mysql+pymysql://root:Sans2024@localhost:3306/demo3"
    engine = create_engine(connection_str)
    df=pd.DataFrame(channel)
    df.to_sql(name='Channels', con=engine, if_exists='append', index=False)
    conn.commit()
    return df
  
def playlist_videos_id(channel_Ids):
  #all_video_ids=[]
  #for channels_id in channel_Ids:
      videos_ids=[]
      response = youtube.channels().list(part="contentDetails",id=channel_Ids).execute()
      playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
  
      nextPageToken=None
  
      while True:
          response2 = youtube.playlistItems().list(part="snippet",playlistId=playlist_Id,maxResults=50,pageToken=nextPageToken).execute()
          for i in range(len(response2["items"])):
            videos_ids.append(response2["items"][i]["snippet"]["resourceId"]["videoId"])
            nextPageToken=response2.get("nextPageToken")
          
          if nextPageToken is None:
              break
      #all_video_ids.extend(videos_ids)        
      return videos_ids   

def videos_data(all_video_ids):
    video_info=[]
   
    for each in all_video_ids:
        request = youtube.videos().list(part='snippet,contentDetails,statistics',id=each)
        response = request.execute()
        for i in response["items"]:
            given= {'Video_Id':i["id"] ,
                    'Video_Title':i["snippet"]["title"],
                    'Video_Description':i["snippet"]["description"],
                    'Channel_Id':i['snippet']['channelId'],
                    'Video_Tags': i['snippet'].get("Tags"),
                    'Video_Pubdate':i["snippet"]["publishedAt"],
                    'Video_Viewcount':i["statistics"]["viewCount"],
                    'Video_Likecount':i["statistics"].get('likeCount') ,
                    'Video_Favoritecount':i["statistics"]['favoriteCount'],
                    'Video_Commentcount':i["statistics"].get('commentCount'),
                    'Video_Duration':duration_to_seconds(i['contentDetails']['duration']),
                    'Video_Thumbnails':i["snippet"]["thumbnails"]['default']['url'],
                    'Video_Caption':i["contentDetails"]["caption"]
            }
                   
            video_info.append(given)
    df1=pd.DataFrame(video_info)  
    #st.write(df1)  
    return df1

def video_mysql_write(video_data):
  df1=video_data
  mydb = sql.connect(host="localhost",user="root",password="Sans2024")
  cursor = mydb.cursor()
  connection_str = "mysql+pymysql://root:Sans2024@localhost:3306/demo3"
  engine = create_engine(connection_str)
  cursor.execute('use demo3')
  cursor.execute('''create table if not exists Videos (  Video_Id VARCHAR(50),
                                                         Video_Title VARCHAR(200),
                                                         Video_Description TEXT,
                                                         Channel_Id VARCHAR(50),
                                                         Video_Tags TEXT, 
                                                         Video_Pubdate VARCHAR(200),
                                                         Video_Viewcount BIGINT,
                                                         Video_Likecount BIGINT,
                                                         Video_Favoritecount INT(15),
                                                         Video_Commentcount BIGINT,
                                                         Video_Duration VARCHAR(20),
                                                         Video_Thumbnails TEXT,
                                                         Video_Caption VARCHAR(10))''')
  mydb.commit()

  df1.to_sql(name='Videos', con=engine, if_exists='append', index=False)

  return df1

def comments_data(v_id):
    comment_data = []
    try:
        next_page_token = None
        for video in v_id:
            while True:
                response = youtube.commentThreads().list(part="snippet",
                                                        videoId=video,
                                                        maxResults=50,
                                                        pageToken=next_page_token).execute()
                #st.write(response)
                for cmt in response['items']:
                    data = {"Comment_Id" : cmt['id'],
                            "Video_Id": cmt['snippet']['videoId'],
                            "Comment_Text" : cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "Comment_Author" : cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_Posted_Date" : cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            "Like_Count" : cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            "Reply_Count" : cmt['snippet']['totalReplyCount']
                    }
                    comment_data.append(data)
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break
    except:
        pass
    df3 = pd.DataFrame(comment_data)
    #st.write(df3)
    return df3

def comment_mysql_write(comment_data):
    
    df4=comment_data 
                
    #Fetched data inserted into MYSQL database using mysql connector
    mydb = sql.connect(host="localhost",user="root",password="Sans2024")

    cursor = mydb.cursor()

    connection_str = f"mysql+pymysql://root:Sans2024@localhost:3306/demo3"
    engine = create_engine(connection_str)

    cursor.execute('use demo3')
    cursor.execute('''create table if not exists Comments (Comment_Id VARCHAR(30),Video_Id VARCHAR(40),Comment_Text TEXT,
                    Comment_Author VARCHAR(255), Comment_Posted_Date VARCHAR(200),Like_Count VARCHAR(50),Reply_Count VARCHAR(50)
                    ) ''')

    mydb.commit()

    df4.to_sql(name='Comments', con=engine, if_exists='append', index=False)

    cursor.close()
    mydb.close()
    return df4

def duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds




st.title("Youtube Data Harvesting & Warehousing")
choice=st.sidebar.radio('Choose an Option',('Welcome','Data Harvesting & warehousing','Querying'))
st.write(choice)
if choice=='Welcome':
    st.title(' Welcome u all')
    #request = youtube.videos().list(part='snippet,contentDetails,statistics',id='vNn_wogzj8M')
    #response = request.execute()
    #st.text(response["items"][0]["statistics"])
if choice=='Data Harvesting & warehousing':
      st.subheader('Youtube Data Harvesting')
      channel_id=st.text_input("Enter your Channel ID:")
      if channel_id:
        channel1=channel_request(channel_id)
        df=channel_mysql_write(channel1)
        st.write("Channel Data harvested")
        st.write(df)
        all_video_list=playlist_videos_id(channel_id)
        video_data=videos_data(all_video_list)
        df2=video_mysql_write(video_data)
        st.write("Video Data harvested")
        st.write(df2)
        comment_data=comments_data(all_video_list)
        c=comment_mysql_write(comment_data)
        st.write("Comment Data harvested")
        st.write(c)
        



if choice=='Querying':
    mydb = sql.connect(host="localhost",user="root",password="Sans2024",database="demo3")

    cursor = mydb.cursor()

    choice1=st.selectbox('Query',['Click the question that you would like to query',
    '1. What are the names of all the videos and their corresponding channels?',
    '2. Which channels have the most number of videos, and how many videos do they have?',
    '3. What are the top 10 most viewed videos and their respective channels?',
    '4. How many comments were made on each video, and what are their corresponding video names?',
    '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
    '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    '7. What is the total number of views for each channel, and what are their corresponding channel names?',
    '8. What are the names of all the channels that have published videos in the year 2022?',
    '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

    if choice1 == '1. What are the names of all the videos and their corresponding channels?':
        cursor.execute("""SELECT Videos.Video_Title,Channels.Channel_Name FROM Videos
                        LEFT JOIN Channels ON Channels.Channel_Id=videos.Channel_Id Order BY Channels.Channel_Name""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
    elif choice1 == '2. Which channels have the most number of videos, and how many videos do they have?':
        cursor.execute("""SELECT Channel_Name 
        AS Channel_Name, Channel_Video AS Total_Videos
                            FROM Channels
                            ORDER BY Channel_Video DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
    elif choice1 == '3. What are the top 10 most viewed videos and their respective channels?':
        cursor.execute("""SELECT Channels.Channel_Name, Video_Title, Video_Viewcount 
                            FROM Videos JOIN Channels ON Channels.Channel_Id=Videos.Channel_Id
                            ORDER BY Video_Viewcount DESC
                            LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
    elif choice1 == '4. How many comments were made on each video, and what are their corresponding video names?':
        cursor.execute("""SELECT a.Video_Title AS Video_Title, b.Total_Comments
                            FROM Videos AS a
                            LEFT JOIN (SELECT Video_Id,COUNT(Comment_Id) AS Total_Comments
                            FROM comments GROUP BY Video_Id) AS b
                            ON a.Video_Id = b.Video_Id
                            ORDER BY b.Total_Comments DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
    
    elif choice1 == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        cursor.execute("""SELECT channels.channel_Name,videos.Video_Title,videos.Video_Likecount FROM videos LEFT JOIN channels 
                            ON channels.channel_Id=videos.Channel_Id Order BY Video_Likecount DESC 
                         LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
    
    elif choice1 == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        cursor.execute("""SELECT Video_Title AS Title, Video_Likecount AS Likes_Count
                            FROM Videos
                            ORDER BY Video_Likecount DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
         
    elif choice1 == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channel_Name ,Channel_View
                            FROM Channels Order BY Channel_Name DESC""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
                
    elif choice1 == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute("""SELECT DISTINCT Channels.Channel_Name
                  FROM Channels
                  JOIN Videos ON Channels.Channel_Id = Videos.Channel_Id
                  WHERE YEAR(Videos.Video_Pubdate) = 2022;
                            """)
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
        
    elif choice1 == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        cursor.execute("""SELECT Channels.Channel_Name,AVG(Video_Duration) 
                  FROM Videos
                  JOIN Channels ON Videos.Channel_Id = Channels.Channel_Id
                  GROUP BY Channel_Name""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
              

        
    elif choice1 == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        cursor.execute("""SELECT Video_Title,Channel_Name,Video_Commentcount
                  FROM Videos
                  JOIN Channels ON Videos.Channel_Id = Channels.Channel_Id
                  ORDER BY Video_Commentcount DESC
                  LIMIT 10""")
        df = pd.DataFrame(cursor.fetchall())
        st.write(df)
        