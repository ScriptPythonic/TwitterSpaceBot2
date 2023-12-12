import requests

bearer_token ="AAAAAAAAAAAAAAAAAAAAAAD0pwEAAAAAzMyx25jM6muo7BvUhUcneldRIyQ%3DfszNn1wi14oeNL0DNzTrYgQjnEnARRtHVx1faO57PQiIqe5uip"
search_url = "https://api.twitter.com/2/spaces/search"
search_term = 'NBA'
params = {'query': search_term,  "space.fields": "created_at,lang,invited_user_ids,participant_count,scheduled_start,started_at,title,topic_ids,updated_at,speaker_ids,ended_at", "expansions": "host_ids,topic_ids,invited_user_ids,creator_id,speaker_ids"}
headers = {
    "Authorization": "Bearer {}".format(bearer_token),
    "User-Agent": "v2SpacesSearchPython"
}
response = requests.request("GET", search_url, headers=headers, params=params)
if response.status_code != 200:
    print(response.json())
else:
    print(response.text)