FROM python:3.6-slim

WORKDIR /app
COPY . /app

# configure and install the required packages
ENV PYTHONPATH="/app:${PYTHONPATH}"
RUN pip install -r requirements.txt

# just spawn the shell
CMD ["/bin/bash"]