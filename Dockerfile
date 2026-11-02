FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml .

RUN pip install .

COPY ./src .

CMD [ "python", "main.py"]
