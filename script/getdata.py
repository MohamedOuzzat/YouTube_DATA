# import requests
import pandas as pd
# from requests_oauthlib import OAuth2Session
# from requests.auth import HTTPBasicAuth
# from oauthlib.oauth2 import BackendApplicationClient
from googleapiclient.discovery import build
import json
import psycopg2


url="https://www.youtube.com/@BroCodez/playlists"



channele_id='UCs2iRkOaPo7QLRKCtiqczEA'

youtube = build('youtube', 'v3', 
                developerKey='AIzaSyAkU2QfUvLWhlxdlqzzR-KkoY8-63RstZQ')

ch_request = youtube.channels().list(
    part='contentDetails',
    id=channele_id)

ch_response = ch_request.execute()


upload_list=ch_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def video_details(video_ids):
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


# def add_statics(df):
# #  df = pd.read_json("YTdata2026-09-14.json")

#  for index, row in df.iterrows():
#      video_id=row["resourceId"]["videoId"] 
#      v=video_details(video_id)

#      df["viewCount"]=v["viewCount"]
#      df["likeCount"]=v["likeCount"]
#      df["favoriteCount"]=v["favoriteCount"]
#      df["commentCount"]=v["commentCount"]

all_snippets = []

def display_vidoes(pl_id):
    # youtube=build('youtube','v3',developerKey='AIzaSyAkU2QfUvLWhlxdlqzzR-KkoY8-63RstZQ')
    nextPageToken=None
    while True:
        pl_request=youtube.playlistItems().list(
                part='snippet',
                playlistId=pl_id,
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

        

    df = pd.DataFrame(all_snippets)
    # add_statics(df)
    df.to_json("./YTdata2026-09-14.json", orient="records", indent=4)
    print(f"Saved {len(all_snippets)} videos.")

        




display_vidoes(upload_list)


def clean_data():
    df = pd.read_json("./YTdata2026-09-14.json")

    df['publishedAt']=pd.to_datetime(df['publishedAt'])
    # drop=df.drop_duplicates()
    df.to_json("YTdata2026-09-14.json",orient="records",indent=4)
    print(df["publishedAt"])
# clean_data()

def _to_int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def save_data_staging():
    with open("test.json") as f:
        file = json.load(f)

    connection = psycopg2.connect(
        host="localhost",
        port=5433,
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

def save_data():

    df=pd.read_json("YTdata2026-09-14.json")


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
    dicframe.to_json("test.json",orient="records",indent=4)
    save_data_staging()
    print(f"Saved {len(dicframe)} records to test.json")
save_data()






# df=pd.DataFrame(video)
# df.to_json("Example_video.json",orient="records",indent=4)
# print(video[""])
""" 
"viewCount": "29566",
        "likeCount": "1171",
        "commentCount": "65"
        """


"""
"viewCount":"8236",
        "likeCount":"224",
        "favoriteCount":"0",
        "commentCount":"47"
"""