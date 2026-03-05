import re
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import string

# Download NLTK data (first time only)
try:
    nltk.data.find('tokenizers/punkt')
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('punkt')
    nltk.download('stopwords')

class TweetCleaner:
    """Clean tweet text for better sentiment analysis"""
    
    @staticmethod
    def clean_tweet_text(text):
        """Clean tweet text by removing URLs, mentions, hashtags, and special characters"""
        if not text:
            return ""
        
        # Remove URLs
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'www\.\S+', '', text)
        
        # Remove mentions (@username)
        text = re.sub(r'@\w+', '', text)
        
        # Remove hashtags (#) but keep the word
        text = re.sub(r'#', '', text)
        
        # Remove RT (retweet) indicators
        text = re.sub(r'RT\s*:', '', text, flags=re.IGNORECASE)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        # Remove punctuation (optional, can keep for context)
        text = text.translate(str.maketrans('', '', string.punctuation))
        
        # Convert to lowercase
        text = text.lower()
        
        # Remove stopwords
        stop_words = set(stopwords.words('english'))
        tokens = word_tokenize(text)
        filtered_tokens = [word for word in tokens if word not in stop_words]
        
        return ' '.join(filtered_tokens)

class SentimentAnalyzer:
    """Analyze sentiment using multiple methods"""
    
    def __init__(self):
        self.vader_analyzer = SentimentIntensityAnalyzer()
        self.cleaner = TweetCleaner()
    
    def analyze_sentiment(self, text):
        """Analyze sentiment using TextBlob and VADER"""
        if not text or text.strip() == "":
            return self._get_empty_sentiment()
        
        # Clean the text first
        cleaned_text = self.cleaner.clean_tweet_text(text)
        
        # TextBlob sentiment
        blob = TextBlob(text)
        textblob_polarity = blob.sentiment.polarity
        textblob_subjectivity = blob.sentiment.subjectivity
        
        # VADER sentiment (works better for social media)
        vader_scores = self.vader_analyzer.polarity_scores(text)
        
        # Determine overall sentiment label
        sentiment_label = self._get_sentiment_label(
            textblob_polarity, 
            vader_scores['compound']
        )
        
        return {
            'original_text': text,
            'cleaned_text': cleaned_text,
            'textblob_polarity': round(textblob_polarity, 4),
            'textblob_subjectivity': round(textblob_subjectivity, 4),
            'vader_compound': round(vader_scores['compound'], 4),
            'vader_positive': round(vader_scores['pos'], 4),
            'vader_negative': round(vader_scores['neg'], 4),
            'vader_neutral': round(vader_scores['neu'], 4),
            'sentiment_label': sentiment_label,
            'confidence_score': round(max(vader_scores['pos'], vader_scores['neg'], vader_scores['neu']), 4)
        }
    
    def _get_sentiment_label(self, textblob_score, vader_compound):
        """Determine sentiment label based on scores"""
        # Use VADER compound score as primary (better for social media)
        if vader_compound >= 0.05:
            return 'POSITIVE'
        elif vader_compound <= -0.05:
            return 'NEGATIVE'
        else:
            return 'NEUTRAL'
    
    def _get_empty_sentiment(self):
        """Return empty sentiment structure for empty text"""
        return {
            'original_text': '',
            'cleaned_text': '',
            'textblob_polarity': 0.0,
            'textblob_subjectivity': 0.0,
            'vader_compound': 0.0,
            'vader_positive': 0.0,
            'vader_negative': 0.0,
            'vader_neutral': 1.0,
            'sentiment_label': 'NEUTRAL',
            'confidence_score': 0.0
        }
    
    def batch_analyze(self, texts):
        """Analyze sentiment for multiple texts"""
        results = []
        for text in texts:
            results.append(self.analyze_sentiment(text))
        return results