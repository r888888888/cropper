#!/usr/bin/env python3

import boto3
from dotenv import load_dotenv, find_dotenv
import re
import tempfile
import time
import requests
import os
import urlparse

load_dotenv(find_dotenv())
queue_url = os.environ.get("AWS_SQS_URL")
sqs = boto3.client("sqs")
loop = True

def build_thumbor_url(url, width, height):
  url = re.sub(r"^https?://", "", url)
  return "http://127.0.0.1/{}x{}/smart/{}".format(width, height, url)

def upload_to_s3(file, key):
  s3 = boto3.client("s3")
  file.seek(0)
  s3.upload_fileobj(file, "danbooru", key, {"ACL" : "public-read"})

def download_and_process(url):
  ext = os.path.splitext(url)[1].lower()
  small_url = build_thumbor_url(url, 200, 200)
  large_url = build_thumbor_url(url, 640, 320)
  small_file = tempfile.NamedTemporaryFile("w+b", suffix=ext)
  large_file = tempfile.NamedTemporaryFile("w+b", suffix=ext)
  for (url, file) in [(small_url, small_file), (large_url, large_file)]:
    with closing(requests.get(url, stream=True)) as resp:
      for chunk in resp.iter_Content(chunk_size=None):
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
        filename = os.path.basename(urlparse.urlparse(url).path)
        print("processing", post_id)
        small_file, large_file = download_and_process(url)
        upload_to_s3(small_file, "cropped/small/{}".format(filename))
        upload_to_s3(large_file, "cropped/large/{}".format(filename))
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
  except:
    print("Error")
    time.sleep(30)
