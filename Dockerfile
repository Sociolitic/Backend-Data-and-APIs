# syntax=docker/dockerfile:1
FROM python:3.8.0
COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip3 install -r requirements.txt
COPY . /app
CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]
