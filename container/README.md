# container
for docker and kubernetes related settings

### Penjelasan folder:
1. **build_1_api**: container dan pod untuk rest api
2. **build_1_cron_legacy**: berisi python module untuk master model data trainer(25 hari)
3. **build_1_cron_daily**: berisi python module untuk online model trainer (per-1 jam, dengan mengambil per-8 jam back time windows)

# deploy step:

### google clound setup
* Run the following command to authenticate to the cluster:

```
    - gcloud container clusters get-credentials kumparan
```

* jika error, coba pastikan kembali sudah memakai credential yang tepat di local pc gcloud sdk anda

```
switch ke kumparan akun:
-----------------------
    - gcloud auth login your.email@kumparan.com
```

* jika masih error, pastikan config project local gcloud sdk anda juga dirubah

```
switch project:
---------------
    gcloud config set project kumparan-data
```

### Docker
* build docker:

```
jangan lupa tanda titik di paling akhir dari command

    - gcloud container builds submit --tag gcr.io/kumparan-data/your_app_service_name:your_app_service_version .

        or for current project:
    - gcloud container builds submit --tag gcr.io/kumparan-data/reco-cron-staging:v0.1.X .
```

### Kubernates 
* build secret:
```
    - kubectl create secret generic your-secret-name --from-file=./topic-recommender-staging.json

    - kubectl --namespace staging create secret generic topicreco-legacytrainer-service-account --from-file=./topic-recommender-staging.json --dry-run -o yaml | kubectl apply -f -

    get list secret:
    ---------------
        - kubectl get secrets
            atau
        - kubectl get secret topicreco-legacytrainer-service-account -o yaml
            atau
        - kubectl --namespace staging get secrets

    descibe secret:
    --------------
        - kubectl describe secrets/topicreco-legacytrainer-service-account
```

### another tutorial
* jika sudah aman, ikuti

[gitlab-kumparan](https://gitlab.kumparan.com/data/k8s-tutorial/blob/master/README.md "gitlab-kumparan")

[quip-kumparan](https://kumparan.quip.com/yI4tAJ3VyoaO "quip-kumparan")

* cek monitor di

[gcloud-console](https://console.cloud.google.com/kubernetes/workload "gcloud-console")

# Note
run docker:

```
docker run -i --rm -v "${PWD}/topic-recommender-staging.json" --env APPLICATION_DEFAULT_CREDENTIALS="4/AAC7JtcB0DRUcq3QHsG34PezjgFRjgJPzYu7q5ywLlFZTH8PfUPzuWo" GOOGLE_APPLICATION_CREDENTIALS="/topic-recommender-staging.json" reco-cron:v0.1.X

docker run -i --rm --env GOOGLE_APPLICATION_CREDENTIALS="/kum_topicreco_cron/src/topic-recommender-staging.json" reco-cron:v0.1.X

docker run --cpus="2" --cap-add=sys_nice --memory=4g -i --rm --env GOOGLE_APPLICATION_CREDENTIALS="/kum_topicreco_cron/src/topic-recommender-staging.json" reco-cron:v0.1.X
```

cek if internet exist on docker:
```
docker run -ti recocron:v0.1.X ping google.com
```

kubernates:

* untuk staging sepertinya lebih baik:
```
restartPolicy: OnFailure -> dirubah ke Never

ini mempermudah mencari bug
```

# some usefull link:
* [stackoverflow-1](https://stackoverflow.com/questions/45697327/load-large-data-from-bigquery-to-python)