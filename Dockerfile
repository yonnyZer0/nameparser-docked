FROM python:2
RUN pip install nameparser unidecode requests
ADD ./Archive ./
CMD [ "python3 -c \"print(\'a\')\"" ]
