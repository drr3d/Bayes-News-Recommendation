FROM python:2.7

RUN apt update

# Creating Application Source Code Directory
RUN mkdir -p /kum_topicreco_cron/src

# Set the working directory
WORKDIR /kum_topicreco_cron/src

# Create a virtualenv for dependencies. This isolates these packages from
# system-level packages.
RUN virtualenv /cron

# Setting these environment variables are the same as running
# source /env/bin/activate.
ENV VIRTUAL_ENV /cron
ENV PATH /cron/bin:$PATH

# Install any needed packages specified in requirements.txt
COPY requirements.txt /kum_topicreco_cron/src
RUN pip install --no-cache-dir -r requirements.txt

# Copy the current directory contents into the container
ADD . /kum_topicreco_cron/src

# Running Python Application
# ~~~ lokal run ~~~
#CMD ["python", "legacy.py", "-ids", "/kum_topicreco_cron/src/", "-cd", "2018-04-15", "-loadmp", "false", "-savetrain", "True", "-savemp", "False", "-storage", "elastic"]

# ~~~ remote run ~~~
CMD ["python", "legacy.py", "-ids", "/kum_topicreco_cron/src/", "-loadmp", "true", "-savetrain", "True", "-savemp", "True", "-storage", "elastic", "-cpu", "3"]
 