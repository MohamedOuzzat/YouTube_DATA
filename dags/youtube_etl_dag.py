# import requests
import pandas as pd
# from requests_oauthlib import OAuth2Session
# from requests.auth import HTTPBasicAuth
# from oauthlib.oauth2 import BackendApplicationClient
from googleapiclient.discovery import build
import json
import psycopg2
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os



CHANNEL_ID = 'UCs2iRkOaPo7QLRKCtiqczEA'
API_KEY    = 'AIzaSyAkU2QfUvLWhlxdlqzzR-KkoY8-63RstZQ'


 

def Retrieve_details():
    youtube = build('youtube', 'v3', 
                    developerKey=API_KEY)
    return youtube
# _________________________________________________________________________
def video_details(video_ids):
    youtube=Retrieve_details()
    details = {}
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        response = youtube.videos().list(
            part='statistics',
            id=','.join(chunk)
        ).execute()
        for item in response["items"]:
            details[item["id"]] = item["statistics"]
    return details


all_snippets = []
# _________________________________________________________________________

def Display_channel():
     os.makedirs('/tmp/data', exist_ok=True)
     youtube=build('youtube','v3',developerKey=API_KEY)
     ch_response = youtube.channels().list(
        part='contentDetails',
        id=CHANNEL_ID
    ).execute()
     return ch_response,youtube

# _________________________________________________________________________

def display_vidoes():
    ch_response, youtube = Display_channel()

    upload_list = ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    nextPageToken=None
    while True:
        pl_request=youtube.playlistItems().list(
                part='snippet',
                playlistId=upload_list,
                maxResults=50,
                pageToken=nextPageToken
            
        )
        pl_response = pl_request.execute()
        nextPageToken = pl_response.get("nextPageToken")
        for item in pl_response["items"]:
                all_snippets.append(item["snippet"])  
            

        nextPageToken = pl_response.get("nextPageToken")
        if not nextPageToken:
            break
    Generate_JSON()

        
# _________________________________________________________________________

def Generate_JSON():
    df = pd.DataFrame(all_snippets)
    # add_statics(df)
    df.to_json("/tmp/data/YTdata2026-09-14.json", orient="records", indent=4)
    print(f"Saved {len(all_snippets)} videos.")

        




# _________________________________________________________________________

def clean_data():
    df = pd.read_json("/tmp/data/YTdata2026-09-14.json")
    df['publishedAt']=pd.to_datetime(df['publishedAt'])
    # drop=df.drop_duplicates()
    df.to_json("/tmp/YTdata2026-09-14.json",orient="records",indent=4)
    print(df["publishedAt"])

    # _________________________________________________________________________


def _to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


# _________________________________________________________________________

def save_data_staging():
    with open("/tmp/data/video_details.json") as f:
        file = json.load(f)

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        database="youtube_db",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    for row in file:
        cursor.execute(
            """
            INSERT INTO staging
            (video_id, title, published_at, view_count, like_count, comment_count, favorite_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO NOTHING
            """,
            (
                row["videoId"],
                row["title"],
                row["publishedAt"],
                _to_int(row.get("viewCount")),
                _to_int(row.get("likeCount")),
                _to_int(row.get("commentCount")),
                _to_int(row.get("favoriteCount")),
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


# _________________________________________________________________________

def save_data():
    os.makedirs('/tmp/data', exist_ok=True)

    df=pd.read_json("/tmp/data/YTdata2026-09-14.json")

    videoId= df["resourceId"].apply(lambda x: x["videoId"])

    details = video_details(videoId.tolist())

    view_count = videoId.apply(lambda x: details.get(x, {}).get("viewCount"))
    likeCount = videoId.apply(lambda x: details.get(x, {}).get("likeCount"))
    commentCount = videoId.apply(lambda x: details.get(x, {}).get("commentCount"))
    dic={
        "videoId":videoId,
        "title":df["title"],
        "publishedAt":df["publishedAt"],
        "viewCount":view_count,
        "likeCount":likeCount,
        "commentCount":commentCount
        }
    dicframe=pd.DataFrame(dic)
    dicframe.to_json("/tmp/data/video_details.json",orient="records",indent=4)
    save_data_staging()
    print(f"Saved {len(dicframe)} records to video_details.json")


# _________________________________________________________________________

def save_data_core():
    with open("/tmp/data/video_details.json") as f:
        file = json.load(f)

    connection = psycopg2.connect(
        host="postgres",
        port=5432,
        database="youtube_db",
        user="postgres",
        password="postgres"
    )
    cursor = connection.cursor()

    for row in file:
        cursor.execute(
            """
            INSERT INTO core
            (video_id, title, published_at, view_count, like_count, comment_count, favorite_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE SET
                view_count    = EXCLUDED.view_count,
                like_count    = EXCLUDED.like_count,
                comment_count = EXCLUDED.comment_count
            """,
            (
                row["videoId"],
                row["title"],
                row["publishedAt"],
                _to_int(row.get("viewCount")),
                _to_int(row.get("likeCount")),
                _to_int(row.get("commentCount")),
                _to_int(row.get("favoriteCount")),
            )
        )

    connection.commit()
    cursor.close()
    connection.close()


with DAG(
    dag_id="youtube_extract",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag1:

    t1= PythonOperator(task_id="Recupere_videos",   python_callable=display_vidoes)
    t2 = PythonOperator(task_id="Recupere_details",  python_callable=save_data)
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_youtube_load",
        trigger_dag_id="youtube_load",
    )

    t1 >> t2 >> trigger_load




with DAG(
    dag_id="youtube_load",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily", 
    catchup=False,
) as dag2:

    t5 = PythonOperator(task_id="clean_data",    python_callable=clean_data)
    t6 = PythonOperator(task_id="load_staging",  python_callable=save_data_staging)
    t7 = PythonOperator(task_id="load_core",     python_callable=save_data_core)

    t5 >> t6 >> t7
  