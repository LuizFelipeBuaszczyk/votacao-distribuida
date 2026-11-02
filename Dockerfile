FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml .

RUN pip install .

ENV PYTHONUNBUFFERED=1

COPY ./src .

RUN python -m grpc_tools.protoc -Iprotocols --python_out=. --grpc_python_out=. protocols/voting.proto

EXPOSE 50051

CMD [ "python", "main.py"]
