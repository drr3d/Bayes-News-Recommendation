#standardSQL
-- Click topic distribution data, t=daily
-- This is for genuine interest in the paper
WITH time_frame AS (
    -- '@date' will be replaced to current date in UTC
    -- by our derived automation server. If you run this query
    -- you need to update '@date' manually to for example '2018-04-03'
    SELECT TIMESTAMP('@date') AS current_date
)
, selected_users AS (
    -- Select only users that have click 10 times in the past 30 days
    SELECT UTCL.user_alias_id AS user_alias_id,
           SUM(DISTINCT UTCL.user_total_click) AS user_total_click
    FROM `topic_recommender.users_total_click` UTCL
    CROSS JOIN time_frame TF
    WHERE UTCL._PARTITIONTIME BETWEEN TIMESTAMP_SUB(TF.current_date, INTERVAL 30 DAY) AND TF.current_date
    GROUP BY 1
    HAVING SUM(DISTINCT UTCL.user_total_click) > 10
)
, click_topic_count_data AS (
    -- Get click count data for each topic, each user
    -- time span: 14 days
    -- TODO: current_date data on tracking events and UNION the results
    SELECT UPDS.user_alias_id AS click_user_alias_id,
           ST.topic_id AS click_topic_id,
           ST.topic_name AS click_topic_name,
           COUNT(ST.topic_name) AS click_topic_count
    FROM `derived.user_page_detail_story` UPDS
    LEFT JOIN `derived.story_topic` ST
    ON ST.story_id=UPDS.story_id
    LEFT JOIN selected_users SU
    ON SU.user_alias_id = UPDS.user_alias_id
    CROSS JOIN time_frame TF
    WHERE UPDS._PARTITIONTIME=TF.current_date
          AND UPDS.user_alias_id IS NOT NULL
          AND SU.user_alias_id IS NOT NULL
          AND ST.topic_id IS NOT NULL
          AND ST.topic_name IS NOT NULL
    GROUP BY 1, 2, 3
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