#standardSQL
-- Click topic distribution data, t=hourly
-- This is for current interest in the paper

WITH click_topic_count_data AS (
    SELECT TE.user_alias_id AS click_user_alias_id,
           ST.topic_id AS click_topic_id,
           ST.topic_name AS click_topic_name,
           COUNT(ST.topic_name) AS click_topic_count
    FROM production.tracking_events TE
    LEFT JOIN `derived.story_topic` ST
    ON ST.story_id=TE.article_id
    WHERE TE._PARTITIONTIME = TIMESTAMP(CURRENT_DATE())
          AND TE.timestamp BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 8 HOUR) AND CURRENT_TIMESTAMP
          AND TE.event_type = 'page_view'
          AND TE.page_type = 'detail_story'
          AND TE.article_id IS NOT NULL
          AND TE.user_alias_id IS NOT NULL
          AND ST.topic_id IS NOT NULL
          AND ST.topic_name IS NOT NULL
    GROUP BY 1,2,3
)
, click_topic_total_data AS (
    -- Get total click for each user
    SELECT CTCD.click_user_alias_id,
           SUM(CTCD.click_topic_count) AS click_topic_total
    FROM click_topic_count_data CTCD
    GROUP BY CTCD.click_user_alias_id
)
-- Get the click distribution data for each user respected to the topic
SELECT CTCD.click_user_alias_id,
       CTCD.click_topic_id,
       CTCD.click_topic_name,
       IF(DGT.topic_name is NULL, FALSE, TRUE) AS click_topic_is_general,
       CTCD.click_topic_count,
       CTTD.click_topic_total,
       (CTCD.click_topic_count/CTTD.click_topic_total) AS click_topic_percentage
FROM click_topic_count_data CTCD
LEFT JOIN click_topic_total_data CTTD
ON CTTD.click_user_alias_id=CTCD.click_user_alias_id
LEFT JOIN `derived.general_topics` DGT
ON DGT.topic_id=CTCD.click_topic_id