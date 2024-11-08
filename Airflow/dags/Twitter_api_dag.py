from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import tweepy
import json
import os
from pathlib import Path
from airflow.models import Variable

def fetch_tweets_from_api(**context):
    """Fetch full tweet data using Twitter API"""
    try:
        # Get bearer token from Airflow Variables or environment
        bearer_token = Variable.get("twitter_bearer_token")  # Replace with your actual token
        
        # Get base directory (Airflow home)
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        base_dir = Path(airflow_home)
        
        # Define input and output paths
        input_file = base_dir / 'data' / 'raw' / 'tweets_and_replies_2024-11-05.json'
        processed_folder = base_dir / 'data' / 'processed'
        
        # Create directories if they don't exist
        processed_folder.mkdir(parents=True, exist_ok=True)
        
        print(f"Reading from: {input_file}")
        
        # Initialize Twitter API client
        client = tweepy.Client(
            bearer_token=bearer_token,
            wait_on_rate_limit=True
        )
        
        # Read tweet IDs
        with open(input_file, 'r') as f:
            data = json.load(f)
            tweet_ids_set = set()
            for item in data['tweets']:
                tweet_ids_set.add(str(item['tweet_id']))
                for reply_id in item.get('replies', []):
                    tweet_ids_set.add(str(reply_id))
            
            tweet_ids = list(tweet_ids_set)
            print(f"Found {len(tweet_ids)} unique Tweet IDs")
        
        # Fetch tweets
        response = client.get_tweets(
            ids=tweet_ids,
            tweet_fields=[
                'created_at',
                'text',
                'public_metrics',
                'lang',
                'conversation_id',
                'in_reply_to_user_id',
                'referenced_tweets'
            ],
            user_fields=[
                'name',
                'username',
                'description',
                'public_metrics',
                'verified'
            ],
            expansions=[
                'author_id',
                'referenced_tweets.id',
                'in_reply_to_user_id',
                'entities.mentions.username'
            ],
            media_fields=[
                'url',
                'public_metrics'
            ]
        )
        
        # Process response
        tweets_data = []
        if response.data:
            tweets_data = [tweet.data for tweet in response.data]
            print(f"Successfully fetched {len(tweets_data)} tweets from API")
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = processed_folder / f'tweets_api_{timestamp}.json'
        
        with open(output_file, 'w') as outfile:
            json.dump(tweets_data, outfile, indent=4)
        
        print(f"Successfully saved API data to {output_file}")
        
        # Push output file path to XCom
        context['task_instance'].xcom_push(key='output_file', value=str(output_file))
        return str(output_file)
        
    except Exception as e:
        print(f"Error fetching tweets: {str(e)}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['your@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'twitter_api_collection',
    default_args=default_args,
    description='Fetch Twitter data using API',
    schedule_interval=None,
    catchup=False
) as dag:

    fetch_tweets = PythonOperator(
        task_id='fetch_tweets_from_api',
        python_callable=fetch_tweets_from_api,
        provide_context=True,
    )
