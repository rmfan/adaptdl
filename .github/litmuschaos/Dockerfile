FROM python:3.7-slim

RUN python3 -m pip install kubernetes
WORKDIR /root
RUN mkdir /source
COPY adaptdl /source/adaptdl
COPY sched /source/sched
COPY examples /source/examples
COPY .github /source/.github
