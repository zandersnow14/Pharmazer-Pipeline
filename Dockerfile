FROM python:latest

COPY requirements.txt .

RUN pip install -r requirements.txt
RUN python -m spacy download en_core_web_sm
RUN python -m pip install 'boto3-stubs[essential]'

COPY grid_data grid_data
COPY comp_data.py .
COPY extract.py .
COPY transform.py .
COPY pipeline.py .

CMD python pipeline.py