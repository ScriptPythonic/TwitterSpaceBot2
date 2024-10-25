import csv
import json
import uuid
from io import BytesIO, StringIO
import zipfile
from typing import List, Dict, Any

import pandas as pd
import requests
import gspread
from flask import Flask, render_template, request, redirect, jsonify, send_file, flash, Blueprint
from apscheduler.schedulers.background import BackgroundScheduler
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Creator: Quadri Basit Ayomide
# Email: basit@example.com

# Initialize Flask app
load = Flask(__name__)
load.secret_key = 'Quadri Basit Ayomide'

# Twitter API Bearer Token
bearer_token = os.getenv("BEARER_TOKEN")
# Scheduler setup
scheduler = BackgroundScheduler()
scheduler.start()

# JSON file path for storing space IDs
json_file_path = 'space_id.json'


#################################### Route For The Homepage ############################################
@load.route('/', methods=['GET', 'POST'])
def home() -> Any:
    """Render the homepage and handle POST requests for space ID submission."""
    if request.method == 'POST':
        space_id = request.form["spaceID"]
        try:
            with open(json_file_path, 'r') as json_file:
                data = json.load(json_file)
        except FileNotFoundError:
            data = []

        # Append the new space_id
        data.append({'space_id': space_id})

        # Write the updated data back to the JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=2)

        params = {
            "expansions": "host_ids,topic_ids,invited_user_ids,creator_id,speaker_ids",
            "space.fields": "created_at,lang,invited_user_ids,participant_count,scheduled_start,started_at,title,topic_ids,updated_at,speaker_ids,ended_at",
            "user.fields": "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld",
        }

        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json",
        }

        response = requests.get(f"https://api.twitter.com/2/spaces/{space_id}", headers=headers, params=params)

        if response.status_code == 200:
            space_data = response.json().get("data", {})
            topics = response.json().get('includes', {}).get('topics', [])

            custom_space_data = {
                'Space Title': space_data.get('title', ''),
                'Topics Ids': ', '.join(map(str, space_data.get('topic_ids', []))),
                'topic_name': [topic.get('name', '') for topic in topics],
                'topic_des': [topic.get('description', '') for topic in topics],
                'Space State': space_data.get('state', ''),
                'Host ID': space_data.get('host_ids', ''),
                'Creator ID': space_data.get('creator_id', ''),
                'Updated At': space_data.get('updated_at', ''),
                'invited_user_ids': space_data.get('invited_user_ids', []),
                'speakers': ', '.join(map(str, space_data.get('speaker_ids', []))),
                'speakersn': len(space_data.get('speaker_ids', [])),
                'Total Moderators': len(space_data.get('speaker_ids', [])),
                'Created At': space_data.get('created_at', ''),
                'Ended At': space_data.get('ended_at', ''),
                'Language': space_data.get('lang', ''),
                'Subscriber Count': space_data.get('participant_count', '')
            }

            for key, value in custom_space_data.items():
                space_data[key] = value

            with open("space_data.json", "w") as space_file:
                json.dump(space_data, space_file, indent=3)

            space_users_data = response.json().get("includes", {}).get("users", [])
            space_user_fields = {
                'id': 'id',
                'name': 'name',
                'username': 'username',
                'created_at': 'created_at',
                'location': 'location',
                'protected': 'protected',
                'public_metrics': 'public_metrics',
                'description': 'description',
                'entities': 'entities',
                'pinned_tweet_id': 'pinned_tweet_id',
                'profile_image_url': 'profile_image_url',
                'url': 'url',
            }

            user_data = []
            for user in space_users_data:
                formatted_user = {user_key: user.get(user_value, '') for user_key, user_value in space_user_fields.items() if user_value not in ['url']}
                user_data.append(formatted_user)

            with open("space_user_data.json", "w") as space_user_file:
                json.dump(user_data, space_user_file, indent=3)

            df = pd.json_normalize(space_users_data)
            eq = pd.json_normalize(space_data)

            keys_to_exclude = [
                'entities.url.urls',
                'entities.description.mentions',
                'entities.description.hashtags',
                'profile_image_url',
                'pinned_tweet_id',
            ]

            rename_mapping = {
                'public_metrics.followers_count': 'Followers',
                'public_metrics.following_count': 'Following',
                'public_metrics.tweet_count': 'Tweet',
                'public_metrics.listed_count': 'Listed',
                'public_metrics.like_count': 'Likes'
            }

            keys_to_expand = ['invited_user_ids', 'topic_ids', 'host_ids']

            desired_order_space_data = [
                'Space Title', 'Topics Ids', 'topic_des', 'topic_name', 'Space State', 'Host ID', 'Creator ID',
                'Updated At', 'invited_user_ids', 'speakers', 'speakersn',
                'Total Moderators', 'Created At', 'Ended At', 'Language', 'Subscriber Count'
            ]

            eq = eq[desired_order_space_data]

            desired_order_user_data = [
                'id', 'name', 'username', 'created_at', 'location', 'protected',
                'description', 'pinned_tweet_id',
                'public_metrics.followers_count',
                'public_metrics.following_count',
                'public_metrics.tweet_count',
                'public_metrics.listed_count',
                'public_metrics.like_count',
            ]

            df = df[desired_order_user_data]

            for key in keys_to_expand:
                if key in eq and not eq[key].empty and all(isinstance(item, list) and item for item in eq[key]):
                    new_columns = [f"{key}_{i + 1}" for i in range(len(eq[key].iloc[0]))]
                    eq[new_columns] = eq[key].apply(lambda x: pd.Series(x) if x else pd.Series([None] * len(new_columns)))
                    eq = eq.drop([key], axis=1, errors='ignore')

            df.rename(columns=rename_mapping, inplace=True)

            df.replace('', None, inplace=True)
            df = df.drop(keys_to_exclude, axis=1, errors='ignore')
            eq = eq.drop([], axis=1, errors='ignore')

            eq.to_csv(f'{space_id}_data.csv', index=False)
            df.to_csv(f'{space_id}_spaceData.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

            return redirect('/report2.html')

        else:
            print(f"Error: {response.status_code}, {response.text}")

    return render_template('index.html')
######################### End of HomePage Route ###################################################


############### Route For Home Page Using Space Title ##################################################
@load.route('/hi', methods=['GET', 'POST'])
def hi() -> Any:
    """Render the homepage with a different route and handle POST requests for space title submission."""
    if request.method == 'POST':
        space_id = request.form["spaceID"]
        try:
            with open(json_file_path, 'r') as json_file:
                data = json.load(json_file)
        except FileNotFoundError:
            data = []

        # Append the new space_id
        data.append({'space_id': space_id})

        # Write the updated data back to the JSON file
        with open(json_file_path, 'w') as json_file:
            json.dump(data, json_file, indent=2)

        params = {
            'query': space_id,
            "space.fields": "created_at,lang,invited_user_ids,participant_count,scheduled_start,started_at,title,topic_ids,updated_at,speaker_ids,ended_at",
            "user.fields": "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld",
            "expansions": "host_ids,topic_ids,invited_user_ids,creator_id,speaker_ids"
        }

        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "v2SpacesSearchPython"
        }
        search_url = "https://api.twitter.com/2/spaces/search"

        response = requests.request("GET", search_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()

            space_data_list = data.get("data", [])
            topics = data.get('includes', {}).get('topics', [])

            formatted_spaces = []
            for space_data in space_data_list:
                custom_space_data = {
                    'Space Title': space_data.get('title', ''),
                    'Topics Ids': ', '.join(map(str, space_data.get('topic_ids', []))),
                    'topic_name': [topic.get('name', '') for topic in topics],
                    'topic_des': [topic.get('description', '') for topic in topics],
                    'Space State': space_data.get('state', ''),
                    'Host ID': space_data.get('host_ids', ''),
                    'Creator ID': space_data.get('creator_id', ''),
                    'Updated At': space_data.get('updated_at', ''),
                    'invited_user_ids': space_data.get('invited_user_ids', []),
                    'speakers': ', '.join(map(str, space_data.get('speaker_ids', []))),
                    'speakersn': len(space_data.get('speaker_ids', [])),
                    'Total Moderators': len(space_data.get('speaker_ids', [])),
                    'Created At': space_data.get('created_at', ''),
                    'Ended At': space_data.get('ended_at', ''),
                    'Language': space_data.get('lang', ''),
                    'Subscriber Count': space_data.get('participant_count', '')
                }
                formatted_spaces.append(custom_space_data)

            for formatted_space in formatted_spaces:
                for key, value in formatted_space.items():
                    space_data[key] = value

            with open("space_data.json", "w") as space_file:
                json.dump(space_data, space_file, indent=3)

            space_users_data = data.get("includes", {}).get("users", [])
            space_user_fields = {
                'id': 'id',
                'name': 'name',
                'username': 'username',
                'created_at': 'created_at',
                'location': 'location',
                'protected': 'protected',
                'public_metrics': 'public_metrics',
                'description': 'description',
                'entities': 'entities',
                'pinned_tweet_id': 'pinned_tweet_id',
                'profile_image_url': 'profile_image_url',
                'url': 'url',
            }

            user_data = []
            for user in space_users_data:
                formatted_user = {user_key: user.get(user_value, '') for user_key, user_value in space_user_fields.items() if user_value not in ['url']}
                user_data.append(formatted_user)

            with open("space_user_data.json", "w") as space_user_file:
                json.dump(user_data, space_user_file, indent=3)

            df = pd.json_normalize(space_users_data)
            eq = pd.json_normalize(space_data)

            keys_to_exclude = [
                'entities.url.urls',
                'entities.description.mentions',
                'entities.description.hashtags',
                'profile_image_url',
                'pinned_tweet_id',
            ]

            rename_mapping = {
                'public_metrics.followers_count': 'Followers',
                'public_metrics.following_count': 'Following',
                'public_metrics.tweet_count': 'Tweet',
                'public_metrics.listed_count': 'Listed',
                'public_metrics.like_count': 'Likes'
            }

            keys_to_expand = ['invited_user_ids', 'topic_ids', 'host_ids']

            desired_order_space_data = [
                'Space Title', 'Topics Ids', 'topic_des', 'topic_name', 'Space State', 'Host ID', 'Creator ID',
                'Updated At', 'invited_user_ids', 'speakers', 'speakersn',
                'Total Moderators', 'Created At', 'Ended At', 'Language', 'Subscriber Count'
            ]

            eq = eq[desired_order_space_data]

            desired_order_user_data = [
                'id', 'name', 'username', 'created_at', 'location', 'protected',
                'description', 'pinned_tweet_id',
                'public_metrics.followers_count',
                'public_metrics.following_count',
                'public_metrics.tweet_count',
                'public_metrics.listed_count',
                'public_metrics.like_count',
            ]

            df = df[desired_order_user_data]

            for key in keys_to_expand:
                if key in eq and not eq[key].empty and all(isinstance(item, list) and item for item in eq[key]):
                    new_columns = [f"{key}_{i + 1}" for i in range(len(eq[key].iloc[0]))]
                    eq[new_columns] = eq[key].apply(lambda x: pd.Series(x) if x else pd.Series([None] * len(new_columns)))
                    eq = eq.drop([key], axis=1, errors='ignore')

            df.rename(columns=rename_mapping, inplace=True)

            df.replace('', None, inplace=True)
            df = df.drop(keys_to_exclude, axis=1, errors='ignore')
            eq = eq.drop([], axis=1, errors='ignore')

            eq.to_csv(f'{space_id}_data.csv', index=False)
            df.to_csv(f'{space_id}_spaceData.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)

            return redirect('/report2.html')

        else:
            print(f"Error: {response.status_code}, {response.text}")

    return render_template('index.html')
######################### End of HomePage Route ###################################################


#######################################Route For The Report Page #######################################
@load.route('/report.html', methods=['GET'])
def report() -> str:
    """Render the report page."""
    return render_template('report.html')
################################ End of Report Page ####################################################

@load.route('/report2.html', methods=['GET'])
def report2() -> str:
    """Render an alternative report page."""
    return render_template('report2.html')
################################ End of Report Page ####################################################


######################################### Route For Testing  #####################################################
@load.route('/test', methods=['GET'])
def test() -> str:
    """Render the test page."""
    return render_template('test.html')
######################### End of Test Page Route ###################################################


######################################### Route For Downloading User Report  #####################################
@load.route('/download', methods=['GET'])
def download() -> Any:
    """Download a zip file containing the user report."""
    df = pd.read_json('space_user_data.json')

    if df.empty:
        return "Error: No data found to create a CSV file."

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('space_user_data.csv', csv_buffer.getvalue())

    zip_buffer.seek(0)

    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='user_report.zip')
######################### End of Download Page Route ###################################################


######################################### Route For Downloading Topic Report  #####################################
@load.route('/download2', methods=['GET'])
def download2() -> Any:
    """Download a zip file containing the topic report."""
    df = pd.read_json('space_data.json')

    if df.empty:
        return "Error: No data found to create a CSV file."

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.writestr('space_data.csv', csv_buffer.getvalue())

    zip_buffer.seek(0)

    return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name='topic_report.zip')
######################### End of Download Page Route ###################################################


if __name__ == "__main__":
    load.run(debug=True)
