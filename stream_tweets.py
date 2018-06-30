# -*- coding: utf-8 -*-
import csv
import datetime
import json
import os
import re
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError
import settings_twitter
import tweepy

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(retries=5)

topic = argentina-iceland-june-16


class StreamListener(tweepy.StreamListener):
    tweet = {}
    httpsCheck = 'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    httpCheck = 'http?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
    idSelf = 0

    def on_status(self, status):
        print("On Status")
        if status.retweeted:
            return
        try:
            self.tweet["userProfile"] = status.user._json[
                "profile_image_url_https"]
            self.idSelf += 1
            self.tweet["tweet"] = status.text.encode('utf8')
            self.tweet["id"] = status.id
            self.tweet["sequence"] = self.idSelf
            self.tweet["created_at"] = status.created_at
            future = producer.send(topic, bytes(self.tweet))
        except Exception as e:
            print e
        finally:
            producer.flush()

    def on_error(self, status_code):
        print "Error, oops"
        if status_code == 420:
            return False

auth = tweepy.OAuthHandler(
    settings_twitter.TWITTER_APP_KEY, settings_twitter.TWITTER_APP_SECRET)
auth.set_access_token(settings_twitter.TWITTER_KEY,
                      settings_twitter.TWITTER_SECRET)
api = tweepy.API(auth)
print "Twitter API Authentication is successful!"
stream_listener = StreamListener()


def startStream():
    print "start stream"
    while True:
        try:
            print "trying"
            stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
            print "Connection made."
            stream.filter(languages=["en"], track=["Mandsaur"])
        except Exception as e:
            print(e)

startStream()
