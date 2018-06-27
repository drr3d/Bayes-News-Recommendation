FROM python:2.7

# Creating Application Source Code Directory
RUN mkdir -p /kum_topicreco_api/src

WORKDIR /kum_topicreco_api/src
EXPOSE 8993

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

#CMD ["gunicorn", "--workers=8", "--bind=0.0.0.0:8993", "-k eventlet", "app:app"]
CMD ["gunicorn", "--workers=8", "--bind=0.0.0.0:8993", "app:app"]