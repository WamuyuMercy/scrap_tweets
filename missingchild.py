import snscrape.modules.twitter as sntwitter
import pandas as pd
import re
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# -------------------------
# 1. Scrape Tweets from @missingchild_ke
# -------------------------
max_tweets = 100 
tweets = []

for i, tweet in enumerate(sntwitter.TwitterUserScraper('missingchild_ke').get_items()):
    if i >= max_tweets:
        break
    tweets.append(tweet)

print(f"Total tweets fetched: {len(tweets)}")

# -------------------------
# 2. Parse Tweet Content
# -------------------------
def parse_tweet(text):
    name = gender = location = status = None

    # Name: Capitalized name pattern
    name_match = re.search(r'([A-Z][a-z]+(?: [A-Z][a-z]+)+)', text)
    if name_match:
        name = name_match.group(0)

    # Gender
    if re.search(r'\bboy\b', text, re.IGNORECASE):
        gender = 'Male'
    elif re.search(r'\bgirl\b', text, re.IGNORECASE):
        gender = 'Female'

    # Location
    location_match = re.search(r'last seen in ([A-Za-z\s]+)', text, re.IGNORECASE)
    if location_match:
        location = location_match.group(1).strip()

    # Status
    if re.search(r'\bhas been found\b|\breunited\b', text, re.IGNORECASE):
        status = 'Found'
    elif re.search(r'\bstill missing\b|\bmissing\b|\blost\b', text, re.IGNORECASE):
        status = 'Missing'

    return name, gender, location, status

parsed_data = []
for tweet in tweets:
    name, gender, location, status = parse_tweet(tweet.content)
    parsed_data.append({
        'Date': tweet.date.strftime('%Y-%m-%d'),
        'Name': name,
        'Gender': gender,
        'Location': location,
        'Status': status,
        'Tweet': tweet.content,
        'Link': tweet.url
    })

print(f"Parsed {len(parsed_data)} tweets with structured data.")

# -------------------------
# 3. Google Sheets Setup
# -------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("C:\\projects\\POC\\creds.json", scope)
client_gs = gspread.authorize(creds)

# Sheet config
sheet = client_gs.open("Missing Children Data 2024").sheet1
sheet.clear()
sheet.append_row(['Date', 'Name', 'Gender', 'Location', 'Status', 'Tweet', 'Link'])

for row in parsed_data:
    sheet.append_row(list(row.values()))

print("Data successfully exported to Google Sheets!")
