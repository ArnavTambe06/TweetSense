# scripts/test_sentiment.py
from src.processing.sentiment_analyzer import SentimentAnalyzer

def test_sentiment():
    analyzer = SentimentAnalyzer()
    
    test_tweets = [
        "I love this project! It's amazing and wonderful!",
        "This is terrible and awful. I hate it.",
        "The weather is okay today, not too bad.",
        "Just saw the new movie, it was fine I guess.",
        "WOW! This is INCREDIBLE! Best thing ever!",
        "Absolutely disappointed. Worst experience of my life."
    ]
    
    print("🧪 Testing Sentiment Analysis:")
    print("=" * 50)
    
    for tweet in test_tweets:
        result = analyzer.analyze_sentiment(tweet)
        print(f"Text: {tweet[:50]}...")
        print(f"  Sentiment: {result['sentiment_label']}")
        print(f"  VADER Score: {result['vader_compound']}")
        print(f"  Confidence: {result['confidence_score']}")
        print("-" * 30)

if __name__ == "__main__":
    test_sentiment()