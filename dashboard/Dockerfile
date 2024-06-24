FROM amd64/python:3.9-slim


#RUN apt-get update && apt-get install -y \
#    postgresql-client \
#    git \
#    wget \
#    && rm -rf /var/lib/apt/lists/*


COPY . /usr/app

WORKDIR /usr/app

RUN pip install -U pip &&\
    pip install -r requirements.txt

