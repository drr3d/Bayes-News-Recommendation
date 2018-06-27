# Getting started:
for query engine that resides on google bigquery

### Penjelasan file:
1. **click_distribution_daily.sql** , mengumpulkan data daily/per-hari yang digunakan **legacy_train**.
2. **click_distribution_hourly.sql**, mengumpulkan data hourly/per-jam(8 hour window) yang digunakan **daily_train**.
3. **user_total_click.sql**, menghitung total click(view) artikel untuk setiap user_id.