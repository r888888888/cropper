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
import base64
from PIL import Image, ImageOps

load_dotenv(find_dotenv())
queue_url = os.environ.get("AWS_SQS_URL")
sqs = boto3.client("sqs")
loop = True

def update_danbooru(post_id):
  params = {"login": os.environ.get("DANBOORU_BOT_LOGIN"), "api_key": os.environ.get("DANBOORU_BOT_API_KEY"), "post[has_cropped]": "true"}
  requests.put("https://danbooru.donmai.us/posts/{}.json".format(post_id), data=params)

def upload_to_s3(file, key):
  s3 = boto3.client("s3")
  file.seek(0)
  s3.upload_fileobj(file, "danbooru", key, {"ACL" : "public-read"})

def download_and_process(url):
  file = tempfile.NamedTemporaryFile("w+b", suffix=".jpg")
  with closing(requests.get(url, stream=True)) as resp:
    for chunk in resp.iter_content(chunk_size=None):
      if chunk:
        file.write(chunk)
  return file

def crop(file, max_width, max_height):
  image = Image.open(file.name)
  centering = get_crop_centering(image.width, image.height)
  preview = ImageOps.fit(image, (max_width, max_height), 0, centering)
  return preview

def get_crop_centering(width, height):
  mn = min(width, height)
  mx = max(width, height)
  if mx / mn >= 4:
    return (0, 0)
  elif mx / min >= 1.5:
    if width > height:
      return (0.33, 0)
    else:
      return (0, 0.33)
  else:
    return (0.5, 0.5)

def print_to_html(file, url):
  file.write("<img src='" + url + "'>")

html = open("/var/www/html/crop.html", "w")
html.write("<html><body>")

while loop:
  try:
    response = sqs.receive_message(QueueUrl=queue_url, WaitTimeSeconds=20)
    if "Messages" in response:
      for message in response["Messages"]:
        receipt_handle = message["ReceiptHandle"]
        post_id, url = message["Body"].split(",")
        filename = re.sub(r".(jpeg|gif|png)", ".jpg", os.path.basename(urllib.parse.urlparse(url).path))
        print("processing", post_id)
        file = download(url)
        cropped = crop(file, 150, 150)
        upload_to_s3(cropped, "cropped/small/{}".format(filename))
        if os.stat(small_file.name).st_size > 0:
          #update_danbooru(post_id)
          print_to_html(html, "cropped/small/{}".format(filename))
        else:
          print("  empty file")
        file.close()
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
  except KeyboardInterrupt:
    print("quitting")
    html.write("</body></html>")
    loop = False
