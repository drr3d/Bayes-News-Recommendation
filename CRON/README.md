Example
---
Contain some stand alone python file to do training example


Legacy Train
---
Contain module for first data training only, still on development

GOOD TO GO.
* `legacy.py` (main python file)
* `modelhanlder.py` (for saving trained model)
* `settings.json` (separate settings file)

we can also use this as daily train, but the drawback is we still load data as N settings.

### setup:
* place `googlenews` package under same directory

### usage example:
```
python legacy.py -ids "D:\\Kerjaan\\python\\Recommender\\topic-recommender\\off_processing\\legacy-train\\" -cd "2018-03-15"

python legacy.py -N 10 -cd "2018-03-15"
```


Daily Train
---
Contain cron job for daily/hourly train, still on development

NO GOOD to go.


Environtment variable
---

### Windows based
[github](https://github.com/GoogleCloudPlatform/nodejs-docs-samples/issues/117 "github")

[itprotoday](http://www.itprotoday.com/management-mobility/powershell-one-liner-getting-local-environment-variables "itprotoday")

[stackoverflow-1](https://stackoverflow.com/questions/39978077/gcloud-exceptions-forbidden-403-missing-or-insufficient-permissions "stackoverflow-1")

[stackoverflow-2](https://stackoverflow.com/questions/47671717/google-api-core-exceptions-forbidden-403-missing-or-insufficient-permissions "stackoverflow-2")

### Linux

### Mac