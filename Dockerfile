FROM python:2
RUN pip install nameparser unidecode requests
ADD ./ ./
CMD [ "python", "parser.py" ]
