#standardSQL
--  Number of users click in a day
WITH time_frame AS (
    -- '@date' will be replaced to current date in UTC
    -- by our derived automation server. If you run this query
    -- you need to update '@date' manually to for example '2018-04-03'
    SELECT TIMESTAMP('@date') AS current_date
)

SELECT UPDS.user_alias_id AS user_alias_id,
       COUNT(DISTINCT UPDS.story_id) AS user_total_click
FROM `derived.user_page_detail_story` UPDS
CROSS JOIN time_frame TF
WHERE UPDS._PARTITIONTIME=TF.current_date
GROUP BY 1
HAVING COUNT(DISTINCT UPDS.story_id) > 1
ORDER BY 1