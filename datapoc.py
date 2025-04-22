import tweepy
import pandas as pd
import re
import time
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --------------------
# 1. Twitter API Setup
# --------------------
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAJid0gEAAAAA8Tggg%2BvenIZFKSzFbxaPdLfoncQ%3DBePozHYh4sprNsicV4rGsvFUSNp545hET4OSoNEKGTHUl68kTU'
client = tweepy.Client(bearer_token=bearer_token)

# -------------------------
# 2. Enhanced Twitter Query
# -------------------------
query = """
("lost child" OR "missing child" OR "help find" OR "last seen" OR "contact police" OR "missing since") 
-(found OR reunited OR game OR toy OR '''"missing item" ''') 
-is:retweet 
-has:media 
lang:en
"""
print("Fetching tweets...")

try:
    response = client.search_recent_tweets(
        query=query,
        max_results=50,
        tweet_fields=['created_at', 'text', 'id'],
        expansions=['author_id']
    )
except tweepy.TooManyRequests:
    print("Rate limit hit! Try again after 15–20 minutes.")
    exit()

tweets = response.data or []
print(f"Total tweets fetched: {len(tweets)}")

# -------------------------
# 3. Enhanced Parsing Logic
# -------------------------
def parse_tweet(text):
    name = gender = location = status = None

    # Enhanced name detection (exclude common false positives)
    name_match = re.search(r'\b([A-Z][a-z]+(?: [A-Z][a-z]+)+)(?=\s*(?:age|\d|year))', text)
    if name_match:
        name = name_match.group(0)

    # Gender detection with context
    gender_terms = {
        'Male': r'\b(boy|male|son)\b',
        'Female': r'\b(girl|female|daughter)\b'
    }
    for gen, pattern in gender_terms.items():
        if re.search(pattern, text, re.IGNORECASE):
            gender = gen
            break

    # Location detection with multiple patterns
    location_patterns = [
        r'last seen (?:in|near) ([A-Za-z\s,]+)',
        r'location:?\s*([A-Za-z\s,]+)',
        r'#([A-Za-z]+)Police'
    ]
    for pattern in location_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            location = match.group(1).strip().rstrip(',')
            break

    # Status detection with timeline awareness
    status_patterns = {
        'Found': r'\b(reunited|found safe|located)\b',
        'Missing': r'\b(still missing|help locate|please share)\b'
    }
    status = 'Missing'  # Default assumption
    for stat, pattern in status_patterns.items():
        if re.search(pattern, text, re.IGNORECASE):
            status = stat
            break

    return name, gender, location, status

# Parse and collect
parsed_data = []
for tweet in tweets:
    name, gender, location, status = parse_tweet(tweet.text)
    parsed_data.append({
        'Date': tweet.created_at.strftime('%Y-%m-%d'),
        'Name': name,
        'Gender': gender,
        'Location': location,
        'Status': status,
        'Tweet': tweet.text,
        'Link': f"https://twitter.com/i/web/status/{tweet.id}"
    })

print(f"Parsed {len(parsed_data)} tweets with structured data.")

# -------------------------
# 4. Google Sheets Setup
# -------------------------
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("C:\projects\POC\creds.json", scope)
client_gs = gspread.authorize(creds)

# Sheet config
sheet = client_gs.open("Missing Children Data 2024").sheet1
sheet.clear()
sheet.append_row(['Date', 'Name', 'Gender', 'Location', 'Status', 'Tweet', 'Link'])

for row in parsed_data:
    sheet.append_row(list(row.values()))

print("Data successfully exported to Google Sheets!")