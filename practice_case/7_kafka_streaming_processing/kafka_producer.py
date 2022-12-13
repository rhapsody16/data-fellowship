from confluent_kafka import Producer
import json
import time
import logging
import tweepy


######### auth token twitter ###########
bearer_token = "xxx"

######### logging config ###########
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

######### logging object ###########
logger = logging.getLogger()
logger.setLevel(logging.INFO)

######### define search term ###########
search_term = "world cup 2022"

######### define a function for Twitter API v2 auth ###########


def authV2(bearer_token, search_terms):
    client = tweepy.Client(bearer_token)
    tweets = client.search_recent_tweets(query=search_term, max_results=100, tweet_fields=[
        'created_at', 'text', 'author_id', 'lang'])
    return tweets


######### create producer object ###########
p = Producer({'bootstrap.servers': 'localhost:9092'})
print('Kafka Producer has been initiated...')

######### define callback ###########


def receipt(err, msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced message on topic {} with value of {}\n'.format(
            msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)


#####################
def main():

    tweets = authV2(bearer_token, search_term)

    for tweet in tweets.data:
        data = {
            'created_at': tweet.created_at,
            'id': tweet.id,
            'author_id': tweet.author_id,
            'lang': tweet.lang,
            'edit_history_tweet_ids': tweet.edit_history_tweet_ids,
            'text': tweet.text

        }
        m = json.dumps(data, default=str)
        p.poll(1)

        # publish Kafka Topic
        p.produce('wc22', m.encode('utf-8'), callback=receipt)
        p.flush()
        time.sleep(3)


if __name__ == '__main__':
    main()
