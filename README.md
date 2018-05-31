Topic Recommendation
===
Topic recommendation dibuat berdasar Artikel Click Behaviour, kemudian dikalkulasikan dengan **Bayesian Framework**.

Reference:
----------
System ini merupakan re-implementasi dari jurnal aslinya yaitu:
[Personalized News Recommendation Based on Click Behavior](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/35599.pdf)

Getting started:
----------------
# API
digunakan untuk REST API, dibuat menggunakan Python Flask

# Container
Docker container dan kubernetes yang digunakan sebagai deployment environtment utama.

# CRON
digunakan sebagai data trainer, terdiri dari 2:
- daily_cron
- legacy_cron
