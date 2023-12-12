import csv

from flask import Flask, render_template,request,json,redirect,jsonify,send_file,Blueprint,flash,send_file
import requests
import pandas as pd
from io import BytesIO
import zipfile
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json
import uuid
from apscheduler.schedulers.background import BackgroundScheduler

json_file_path = 'space_id.json'
scheduler = BackgroundScheduler()
scheduler.start()

load = Flask(__name__)
load.secret_key = 'Quadri Basit Ayomide'

bearer_token = "AAAAAAAAAAAAAAAAAAAAAGySqgEAAAAA0JqsjgfL1SbzyhyABcKXE4uSaac%3DJAYEPUd81SKOqZiJNQII9VEN9P2CCoe2VusnSKlzyKSjpLGgHa"

#################################### Route For The Homepage ############################################
@load.route('/', methods=['GET', 'POST'])
def home():
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
            

       
            space_data = data.get("data", {})
            topics = data['includes']['topics']


            custom_space_data = {
                'Space Title': space_data.get('title', ''),
                'Topics Ids': ', '.join(map(str, space_data.get('topic_ids', []))),
                'topic_name': [topic.get('name', '') for topic in topics],
                'topic_des': [topic.get('description', '') for topic in topics],
                'Space State': space_data.get('state', ''),
                'Host ID': space_data.get('host_ids', ''),
                'Creator ID' : space_data.get('creator_id', ''),
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
                formatted_user = {}
                for user_key, user_value in space_user_fields.items():
                     if user_value not in ['url']:
                        formatted_user[user_key] = user.get(user_value, '')
                user_data.append(formatted_user)
                
            with open("space_user_data.json", "w") as space_user_file:
                json.dump(user_data, space_user_file, indent=3)
                df =pd.json_normalize(space_users_data)
                eq =pd.json_normalize(space_data)
                
                keys_to_exclude = [
                    'entities.url.urls',
                    'entities.description.mentions',
                    'entities.description.hashtags',
                    'profile_image_url',
                    'pinned_tweet_id',
                ]
                
                keys_to_exclude2 = [

                ]
                
                rename_mapping = {
                    'public_metrics.followers_count':'Followers',
                     'public_metrics.following_count': 'Following',
                    'public_metrics.tweet_count': 'Tweet',
                    'public_metrics.listed_count': 'Listed',
                    'public_metrics.like_count':'Likes'
                }
    
                keys_to_expand = ['invited_user_ids', 'topic_ids', 'host_ids']
                
                desired_order_space_data = [
                    'Space Title', 'Topics Ids','topic_des', 'topic_name', 'Space State','Host ID','Creator ID',
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
                
                eq=eq.drop(keys_to_exclude2, axis=1, errors='ignore')
                
                
                
                
                eq.to_csv(f'{space_id}_data.csv', index=False)
                df.to_csv(f'{space_id}_spaceData.csv', index=False,quoting=csv.QUOTE_NONNUMERIC)
                return redirect('/report2.html')
                
        else:
            print(f"Error: {response.status_code}, {response.text}")
    
    return render_template('index.html')
######################### End of HomePage Route ###################################################

############### Route For Home Page Using Space Title ##################################################
@load.route('/hi', methods=['GET', 'POST'])
def hi():
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

        params = {'query': space_id,  "space.fields": "created_at,lang,invited_user_ids,participant_count,scheduled_start,started_at,title,topic_ids,updated_at,speaker_ids,ended_at","user.fields": "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,verified_type,withheld", "expansions": "host_ids,topic_ids,invited_user_ids,creator_id,speaker_ids"}

        headers = {
            "Authorization": "Bearer {}".format(bearer_token),
            "User-Agent": "v2SpacesSearchPython"
        }
        search_url = "https://api.twitter.com/2/spaces/search"

        response = requests.request("GET", search_url, headers=headers, params=params)

        if response.status_code == 200:
            
            data = response.json()

            space_data_list = data.get("data", [])
            topics = data.get('includes', {}).get('topics', [])

            space_datas = []
            for space_data in space_data_list:
                custom_space_data = {
                    'Space_Title': space_data.get('title', ''),
                    'Participant' : space_data.get('participant_count', ''),
                    'ID' : space_data.get('id', ''),
                    'Topics_Ids': ', '.join(map(str, space_data.get('topic_ids', []))),
                    'topic_name': [topic.get('name', '') for topic in topics],
                    'topic_des': [topic.get('description', '') for topic in topics],
                    'State': space_data.get('state', ''),
                    'Host': ', '.join(map(str, space_data.get('host_ids', []))),
                    'Creator ID': space_data.get('creator_id', ''),
                    'Updated': space_data.get('updated_at', ''),
                    'Scheduled': space_data.get('scheduled_start', ''),
                    'invited_user_ids': ', '.join(map(str, space_data.get('invited_user_ids', []))),
                    'speakers': ', '.join(map(str, space_data.get('speaker_ids', []))),
                    'speakersn': len(space_data.get('speaker_ids', [])),
                    'Total Moderators': len(space_data.get('speaker_ids', [])),
                    'Created': space_data.get('created_at', ''),
                    'Started': space_data.get('started_at', ''),
                    'Ended': space_data.get('ended_at', ''),
                    'Language': space_data.get('lang', ''),
                    'Subscriber': space_data.get('participant_count', '')
                }

                formatted_space = {}
                for space_key, space_value in custom_space_data.items():
                    if space_value not in ['url']:
                        formatted_space[space_key] = space_value

                space_datas.append(formatted_space)

            with open("space_data.json", "w") as space_file:
                json.dump(space_datas, space_file, indent=3)

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
                formatted_user = {}
                for user_key, user_value in space_user_fields.items():
                    if user_value not in ['url']:
                        formatted_user[user_key] = user.get(user_value, '')
                user_data.append(formatted_user)

            with open("space_user_data.json", "w") as space_user_file:
                json.dump(user_data, space_user_file, indent=3)
                df = pd.json_normalize(space_users_data)
                eq = pd.json_normalize(space_datas)

                keys_to_exclude = [
                    'entities.url.urls',
                    'entities.description.mentions',
                    'entities.description.hashtags',
                    'profile_image_url',
                    'pinned_tweet_id',
                ]

                keys_to_exclude2 = [

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
                    'Space_Title', 'Participant','ID', 'Topics_Ids', 'topic_des', 'topic_name', 'State', 'Host', 'Creator ID','Started',
                    'Updated','Scheduled', 'invited_user_ids', 'speakers', 'speakersn',
                    'Total Moderators', 'Created', 'Ended', 'Language', 'Subscriber'
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
                        eq[new_columns] = eq[key].apply(
                            lambda x: pd.Series(x) if x else pd.Series([None] * len(new_columns)))

                        eq = eq.drop([key], axis=1, errors='ignore')

                df.rename(columns=rename_mapping, inplace=True)

                df.replace('', None, inplace=True)

                df = df.drop(keys_to_exclude, axis=1, errors='ignore')

                eq = eq.drop(keys_to_exclude2, axis=1, errors='ignore')

                eq.to_csv(f'{space_id}_data.csv', index=False)
                df.to_csv(f'{space_id}_spaceData.csv', index=False, quoting=csv.QUOTE_NONNUMERIC)
                return redirect('/report.html')

        else:
            print(f"Error: {response.status_code}, {response.text}")

    return render_template('index.html')
####################### End Of Homepage Route using Space Title #####################################

@load.route('/report2.html')
def reports():
    return render_template('report2.html')
@load.route('/report.html')
def report():
    return render_template('report.html')

@load.route('/get_space_data')
def get_space_data():
    with open('space_data.json', 'r') as file:
        space_data = json.load(file)
    return jsonify(space_data)

@load.route('/get_tweets')
def annotation():
    with open('tweets.json', 'r',encoding='utf-8') as file:
        annotation = json.load(file)
    return jsonify(annotation)


@load.route('/get_space_user_data')
def get_space_user_data():
    with open('space_user_data.json', 'r') as file:
       space_user_data = json.load(file)
    return jsonify(space_user_data)


@load.route('/space_user_dataJson')
def download_space_user():
    filename = 'space_user_data.json'
    return send_file(filename, as_attachment=True)

@load.route('/space_user_dataCsv')
def download_space_userCsv():
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
        last_space_id = data[-1]['space_id']
    filepath = f'{last_space_id}_spaceData.csv'
    return send_file(filepath, as_attachment=True)

@load.route('/space_dataJson')
def download_space():
    filename = 'space_data.json'
    return send_file(filename, as_attachment=True)

@load.route('/space_dataCsv')
def download_spaceCsv():
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
        last_space_id = data[-1]['space_id']
    filepath = f'{last_space_id}_data.csv'
    return send_file(filepath, as_attachment=True)

@load.route('/download_files')
def download_files():
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
        last_space_id = data[-1]['space_id']
    files_to_download = ['space_data.json', f'{last_space_id}_spaceData.csv', f'{last_space_id}_data.csv', 'space_user_data.json']
    zip_buffer = BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for file in files_to_download:
            zip_file.write(file)

    zip_buffer.seek(0)

    return send_file(zip_buffer, as_attachment=True, download_name='data.zip')
from io import StringIO
def update_or_create_worksheet(spreadsheet, worksheet_name, csv_contents):
        try: 
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(worksheet_name, rows=100, cols=22)
        
        worksheet.clear()

        
        # values = [csv.split('"') for csv in csv_contents.split('\n')]
        reader = csv.reader(StringIO(csv_contents))
        values = [row for row in reader]
        worksheet.update(values=values, range_name=None)

@load.route('/upload_to_sheet')
def spreadsheets():
    json_file_path = 'space_id.json'
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
        last_space_id = data[-1]['space_id']
    scope = [
        'https://www.googleapis.com/auth/drive',
        'https://www.googleapis.com/auth/spreadsheets'
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name('local-market-6b270-8156f3ace3a3.json', scope)
    client = gspread.authorize(creds)

    spreadsheet = client.open("space user")

    with open(f'{last_space_id}_spaceData.csv', 'r', encoding="utf-8") as file1:
        csv_contents1 = file1.read()

    with open(f'{last_space_id}_data.csv', 'r', encoding="utf-8") as file2:
        csv_contents2 = file2.read()
    json_file_path = "space_id.json"

    # Read the JSON file
    with open(json_file_path, "r") as json_file:
        data = json.load(json_file)
        last_space_id = data[-1]['space_id']

    worksheet1_name = f'{last_space_id}_spaceUserdata'
    worksheet2_name = f'{last_space_id}_spaceData'

    update_or_create_worksheet(spreadsheet, worksheet1_name, csv_contents1)
    update_or_create_worksheet(spreadsheet, worksheet2_name, csv_contents2)
    
    flash('Uploaded sucessfully !!!','success')
    return redirect("/")



#### adding the context annotation
def extract_ids_from_json(json_file):
    try:
        with open('space_user_data.json', 'r', encoding='utf-8') as file:
            data = json.load(file)
            user_ids = [item['id'] for item in data]
            print(user_ids)
            return user_ids
    except FileNotFoundError:
        print(f"Error: File '{json_file}' not found.")
        return []
#fetch usuer tweets and annotation
def fetch_tweets_and_annotations(user_ids, max_results, bearer_token, output_json_file):
    user_tweets = {} 

    for user_id in user_ids:
        url = f'https://api.twitter.com/2/users/{user_id}/tweets?tweet.fields=context_annotations&exclude=retweets,replies&max_results={max_results}'

        headers = {
            'Authorization': f'Bearer {bearer_token}'
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            user_tweets[user_id] = data.get('data', [])
        else:
            print(f"Error {response.status_code} for user {user_id}: {response.text}")

    with open(output_json_file, 'w', encoding='utf-8') as json_file:
        json.dump(user_tweets, json_file, ensure_ascii=False, indent=4)

# Example usage
max_results = 5
spacedata_json_file = 'space_user_data.json'
output_json_file = 'tweets.json'

# Extract user IDs from spacedata.json
user_ids = extract_ids_from_json(spacedata_json_file)
#fetch_tweets_and_annotations(user_ids, max_results, bearer_token, output_json_file)

scheduler.add_job(spreadsheets, 'interval', hours=24)

if __name__ == "__main__":
    load.run(host='localhost', port=5000, debug=True)