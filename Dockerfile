FROM python:3.6-slim

WORKDIR /app

# configure and install the required packages
ENV PYTHONPATH="/app:${PYTHONPATH}"
COPY ./requirements.txt /app
RUN pip install -r requirements.txt

# copy the source and config files
COPY ./ingester /app/ingester
COPY ./config /app/config
COPY ./scripts/ /app

# just spawn the shell
CMD ["/bin/bash"]