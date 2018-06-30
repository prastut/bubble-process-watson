# -*- coding: utf-8 -*-
def call_watson_pickably(tweet):
    call_watson(tweet)

from collections import defaultdict
import sys
import ast
import csv
import datetime
from itertools import chain
import json
from multiprocessing import Pool
import os
import re
import time

from entity_dict import entity_dict_CSK, entity_dict_SRH, entity_dict_ARG, entity_dict_ISL 

from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka import KafkaConsumer
import pymongo
from pymongo import MongoClient
import settings_twitter
import settings_watson
import tweepy
from watson_developer_cloud import NaturalLanguageUnderstandingV1
from watson_developer_cloud.natural_language_understanding_v1 import Features, EntitiesOptions, KeywordsOptions, CategoriesOptions


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(retries=5)
response_batch = []

natural_language_understanding = NaturalLanguageUnderstandingV1(
    username=settings_watson.username,
    password=settings_watson.password,
    version='2018-03-16')

date = "2018-06-16"

entity_dict = defaultdict(list)
for k, v in chain(entity_dict_ARG.items(), entity_dict_ISL.items()):
    entity_dict[k].append(v)
for k, v in entity_dict.items():
    print(k, v)

def connect_mongo():
    client = MongoClient('localhost', 27017)
    db = client["Bubble"]
    return db


def call_watson(tweet):
    try:
        response = natural_language_understanding.analyze(
            text=tweet["tweet"],
            features=Features(
                entities=EntitiesOptions(
                    emotion=True,
                    sentiment=True,
                    limit=2),
                keywords=KeywordsOptions(
                    emotion=True,
                    sentiment=True,
                    limit=2)),
            language='en'
            )
        response["tweet"] = tweet["tweet"]
        response["tweetId"] = tweet["id"]
        response["timeStamp"] = tweet["created_at"]
        save_results(response)
    except Exception as e:
        print(e)
    
    
def save_results(response):
    print("Saving results.")
    db = connect_mongo()
    entities_tweet = response["entities"]
    for entity in entities_tweet:
        try:
            for i in entity_dict:
                for j in entity_dict[i]:
                    if(entity["text"] in j):
                        entity["tweet"] = response["tweet"]
                        entity["tweetId"] = response["tweetId"]
                        entity["timeStamp"] = response["timeStamp"]
                        db.argentinaIceland.insert(entity)
                        print("Inserted.")
                    else:
                        print("All ignored.")
        except Exception as e:
            print (e)

tweets_batch = []

def main():
    p = Pool(4)
    consumer = KafkaConsumer('argentina-iceland-june-16', bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
    for message in consumer:
        tweet = (message.value)
        tweet.replace('\n', '\\n')
        tweet = eval(tweet)
        tweets_batch.append(tweet)
        if(len(tweets_batch)==4):
            p.map(call_watson_pickably, tweets_batch)
            del tweets_batch[:]
    
if __name__ == "__main__":
    main()