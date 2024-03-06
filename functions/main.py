# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn
from firebase_admin import initialize_app, storage, credentials

import re
import pandas as pd
import requests
import os
import io
import urllib.parse

from generate_item import generate_item
from generate_daily_visits import generate_daily_visits
from generate_hourly_visits import generate_hourly_visits

debug_url = os.environ.get("DEBUG_URL")
spreadsheet_url = os.environ.get("SPREADSHEET_URL")

cred = credentials.Certificate("credentials.json")

initialize_app(cred, {
    "storageBucket": "testcalnourish.appspot.com"
})

@https_fn.on_request()
def fetch_upload_data(req: https_fn.Request) -> https_fn.Response:
    pattern = r'https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)(/edit#gid=(\d+)|/edit.*)?'

    replacement = lambda m: f'https://docs.google.com/spreadsheets/d/{m.group(1)}/export?' + (f'gid={m.group(3)}&' if m.group(3) else '') + 'format=csv'

    new_url = re.sub(pattern, replacement, spreadsheet_url)

    df = pd.read_csv(new_url)

    bucket = storage.bucket()

    ref = "pantry-data.csv"
    blob = bucket.blob(ref)
    blob.upload_from_string(df.to_csv())

    return https_fn.Response(ref)

@https_fn.on_request()
def generate_item_visualization(req: https_fn.Request) -> https_fn.Response:
    qs = req.query_string.decode("utf-8")
    
    if not qs:
        return https_fn.Response()

    parsed_qs = dict(urllib.parse.parse_qs(qs))

    bucket = storage.bucket()

    blob = bucket.blob("pantry-data.csv")
    contents = blob.download_as_string()

    df = pd.read_csv(io.StringIO(contents.decode("utf-8")), sep=",")

    data = str(generate_item(df, parsed_qs.get("item")[0], int(parsed_qs.get("week-history")[0])))

    return https_fn.Response(data)

@https_fn.on_request()
def generate_hourly_visits_visualization(req: https_fn.Request) -> https_fn.Response:
    qs = req.query_string.decode("utf-8")
    
    if not qs:
        return https_fn.Response()

    parsed_qs = dict(urllib.parse.parse_qs(qs))

    bucket = storage.bucket()

    blob = bucket.blob("pantry-data.csv")
    contents = blob.download_as_string()

    df = pd.read_csv(io.StringIO(contents.decode("utf-8")), sep=",")

    data = str(generate_hourly_visits(df, parsed_qs.get("on-weekday")[0], int(parsed_qs.get("week-history")[0])))

    return https_fn.Response(data)
