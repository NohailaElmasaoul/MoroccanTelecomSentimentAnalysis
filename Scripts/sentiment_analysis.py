import json
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk

# Download the VADER lexicon if you haven't already
nltk.download('vader_lexicon')

# Initialize the SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()

# Load your JSON file
with open("tweets_response1.json", "r") as f:
    data = json.load(f)

# Iterate over each tweet and calculate the sentiment score
for tweet in data:
    text = tweet.get("text", "")  # Assuming the tweet text is in a 'text' field
    score = analyzer.polarity_scores(text)["compound"]
    # Normalize to 0–1 range
    normalized_score = (score + 1) / 2
    # Add the score to the tweet data
    tweet["sentiment_score"] = normalized_score

# Optionally save the updated JSON
with open("tweets_with_sentiment.json", "w") as f:
    json.dump(data, f, indent=2)
