FROM python:3.7.2-slim
ADD . .
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
RUN apt-get update
RUN apt-get install git -y
RUN git submodule init
RUN git submodule update --recursive
CMD ["python3", "etherfx_worker_daemon.py"]
