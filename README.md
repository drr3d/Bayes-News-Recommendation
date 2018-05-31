# Topic Recommendation

Topic recommendation dibuat berdasar Artikel Click Behaviour, kemudian dikalkulasikan dengan **Bayesian Framework**.

# Reference:
System ini merupakan re-implementasi dari jurnal aslinya yaitu:

* [Personalized News Recommendation Based on Click Behavior](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/35599.pdf)

# Getting started:
Penjelasan folder:

### API
digunakan untuk REST API, dibuat menggunakan Python Flask

### container
Docker container dan kubernetes yang digunakan sebagai deployment environtment utama.

### CRON
digunakan sebagai data trainer, terdiri dari 2:
- daily_cron
- legacy_cron

### google-bigquery
berisi file-file sql-query yang digunakan pada proses awal, query tersebut digunakan untuk membuat 2 table data feeder utama yaitu:
1. topic_recommender.click_distribution_hourly

        table ini berisikan data hourly

2. topic_recommender.click_distribution_daily

    table ini berisikan data daily

### src
Berisi file class utama perhitungan
