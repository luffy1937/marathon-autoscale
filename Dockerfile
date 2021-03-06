#
# Docker image that can be run under Marathon management to dynamically scale a Marathon service running on DC/OS.
#

FROM python:3-alpine

# Copy the python scripts into the working directory
ADD / /marathon-autoscale
WORKDIR /marathon-autoscale

RUN apk add --update --virtual .build-dependencies openssl-dev libffi-dev make gcc g++
RUN apk add --virtual python-dev
RUN apk add tzdata \
  && cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
  && apk del tzdata \
  && rm -rf /var/cache/apk/*

RUN pip install -r requirements.txt

# Start the autoscale application
CMD python marathon_autoscaler.py
