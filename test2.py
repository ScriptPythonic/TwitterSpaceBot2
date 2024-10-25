import requests
import os
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Get the token from environment variables
bearer_token = os.getenv("BEARER_TOKEN")
search_url = "https://api.twitter.com/2/spaces/search"
search_term = 'NBA'
params = {
    'query': search_term,
    "space.fields": "created_at,lang,invited_user_ids,participant_count,scheduled_start,started_at,title,topic_ids,updated_at,speaker_ids,ended_at",
    "expansions": "host_ids,topic_ids,invited_user_ids,creator_id,speaker_ids"
}
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "User-Agent": "v2SpacesSearchPython"
}

response = requests.request("GET", search_url, headers=headers, params=params)

if response.status_code != 200:
    print(response.json())
else:
    print(response.text)
