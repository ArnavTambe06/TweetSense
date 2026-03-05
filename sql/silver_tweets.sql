-- Run in BigQuery Console
SELECT sentiment_label,
    COUNT(*) as count,
    ROUND(AVG(vader_compound), 3) as avg_sentiment,
    ROUND(AVG(confidence_score), 3) as avg_confidence
FROM `twitter_silver.tweet_sentiment`
GROUP BY sentiment_label
ORDER BY count DESC