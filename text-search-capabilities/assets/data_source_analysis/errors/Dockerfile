FROM python:3.8-slim

ARG SPACY_MODEL

RUN apt -y update;\
    apt -y install openjdk-11-jre-headless

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY index.py requirements.txt ./

RUN pip3 install -U pip setuptools wheel
RUN python3.8 -m pip install -r requirements.txt -t .
RUN python -m spacy download $SPACY_MODEL

ENTRYPOINT ["python3", "index.py"]
