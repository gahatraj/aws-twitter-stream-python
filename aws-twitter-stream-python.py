from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time
import os
import config

class TweetStreamListener(StreamListener):
    # on success
    def on_data(self, data):
        tweet = json.loads(data)
        try:
            if 'text' in tweet.keys():
                message_lst = [str(tweet['id']),
                       str(tweet['user']['name']),
                       str(tweet['user']['screen_name']),
                       tweet['text'].replace('\n',' ').replace('\r',' '),
                       str(tweet['user']['followers_count']),
                       str(tweet['user']['location']),
                       str(tweet['geo']),
                       str(tweet['created_at']),
                       '\n'
                       ]
                message = '\t'.join(message_lst)
                firehose_client.put_record(
                    DeliveryStreamName=kinesis_stream_name,
                    Record={
                        'Data': message
                    }
                )
        except (AttributeError, Exception) as e:
            print(e)
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # kinesis connection
    session = boto3.Session(profile_name='profilename')
    firehose_client = session.client('firehose', region_name='us-east-1')

    # Kinesis stream name
    kinesis_stream_name = 'firehose-data-stream-name'

    # twitter/aws credentials
    consumer_key = config.consumer_key
    consumer_secret = config.consumer_secret
    access_token = config.access_token
    access_token_secret = config.access_token_secret

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        try:
            print('Twitter streaming...')

            # tweet stream listener
            listener = TweetStreamListener()

            # tweepy stream instance
            stream = Stream(auth, listener)

            # search twitter for the keyword
            stream.filter(track=['CFC'], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(10)
            continue

