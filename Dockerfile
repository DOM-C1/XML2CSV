FROM python:latest
COPY requirements.txt .
COPY main.py .
COPY institutes.csv .
COPY aliases.csv .
RUN pip3 install -r requirements.txt
RUN python -m spacy download en_core_web_sm
CMD python3 main.py