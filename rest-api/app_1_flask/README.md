flask-api
---

## Run Docker locally:
build docker:
```
    - gcloud container builds submit --tag gcr.io/kumparan-data/reco-api-staging:v0.1.X .
```

method

```
docker run -it -d --name reco_api -p 8993:8993 --env GOOGLE_APPLICATION_CREDENTIALS="/kum_topicreco_api/src/topic-recommender-staging.json" reco-api-staging:v0.1.X

masuk console
-------------
docker exec -it 4e427383a48da1c39fb3435df10652ba938e03a9a93a8aadf5133ec628804617 /bin/bash

hapus container
---------------
docker container rm -f 4e427383a48da1c39fb3435df10652ba938e03a9a93a8aadf5133ec628804617
```

expose service via kubernetes

```
kubectl --namespace staging expose deployment topic-reco-flaskapi --type=LoadBalancer --port 80 --target-port 8993
```

## Usefull link

https://github.com/honestbee/flask_app_k8s

https://stackoverflow.com/questions/43925487/how-to-run-gunicorn-on-docker

https://medium.com/@trstringer/debugging-a-python-flask-application-in-a-container-with-docker-compose-fa5be981ec9a

https://martinapugliese.github.io/python-for-(some)-elasticsearch-queries/

https://www.elastic.co/guide/en/elasticsearch/reference/6.2/query-dsl-match-query.html

