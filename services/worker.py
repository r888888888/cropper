#!/usr/bin/env python3

import boto3
from dotenv import load_dotenv, find_dotenv
import re
import tempfile
import time
import requests
import os
import urllib
from contextlib import closing
import hmac
import base64
import hashlib

load_dotenv(find_dotenv())
queue_url = os.environ.get("AWS_SQS_URL")
sqs = boto3.client("sqs")
loop = True
thumbor_key = open("/etc/thumbor.key", "r").read().encode()

def update_danbooru(post_id):
  params = {"login": os.environ.get("DANBOORU_BOT_LOGIN"), "api_key": os.environ.get("DANBOORU_BOT_API_KEY")}
  requests.post("https://danbooru.donmai.us/posts/{}".format(post_id), data=params)

def build_hmac(x):
  return base64.urlsafe_b64encode(hmac.new(thumbor_key, x.encode(), digestmod=hashlib.sha1).digest()).decode("utf-8")

def build_thumbor_url(url, width, height):
  url = re.sub(r"^https?://", "", url)
  filters = "filters:format(jpeg)"
  path = "{}x{}/smart/{}/{}".format(width, height, filters, url)
  hmac = build_hmac(path)
  return "http://127.0.0.1:8888/{}/{}".format(hmac, path)

def upload_to_s3(file, key):
  s3 = boto3.client("s3")
  file.seek(0)
  s3.upload_fileobj(file, "danbooru", key, {"ACL" : "public-read"})

def download_and_process(url):
  ext = os.path.splitext(url)[1].lower()
  small_url = build_thumbor_url(url, 150, 150)
  large_url = build_thumbor_url(url, 640, 320)
  small_file = tempfile.NamedTemporaryFile("w+b", suffix="jpg")
  large_file = tempfile.NamedTemporaryFile("w+b", suffix="jpg")
  for (url, file) in [(small_url, small_file), (large_url, large_file)]:
    with closing(requests.get(url, stream=True)) as resp:
      for chunk in resp.iter_content(chunk_size=None):
        if chunk:
          file.write(chunk)
  return (small_file, large_file)

while loop:
  try:
    response = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=20)
    if "Messages" in response:
      for message in response["Messages"]:
        receipt_handle = message["ReceiptHandle"]
        post_id, url = message["Body"].split(",")
        filename = re.sub(r".(jpeg|gif|png)", ".jpg", os.path.basename(urllib.parse.urlparse(url).path))
        print("processing", post_id)
        small_file, large_file = download_and_process(url)
        upload_to_s3(small_file, "cropped/small/{}".format(filename))
        upload_to_s3(large_file, "cropped/large/{}".format(filename))
        small_file.close()
        large_file.close()
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        update_danbooru(post_id)
  except:
    print("Error")
    time.sleep(30)
