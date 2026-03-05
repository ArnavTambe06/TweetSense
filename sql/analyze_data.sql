SELECT tweet_id,
    SUBSTR(original_text, 1, 50) as text_preview,
    sentiment_label,
    vader_compound,
    confidence_score,
    processed_at
FROM `twitter_silver.tweet_sentiment`
ORDER BY processed_at DESC
LIMIT 10