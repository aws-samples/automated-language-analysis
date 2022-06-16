FROM public.ecr.aws/lambda/python:3.9

ARG SPACY_MODEL

COPY index.py requirements.txt ./

RUN pip3 install -U pip setuptools wheel
RUN python3.9 -m pip install -r requirements.txt -t .
RUN python -m spacy download $SPACY_MODEL

ENTRYPOINT ["python3", "index.py"]
