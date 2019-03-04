FROM python:3.6-stretch

MAINTAINER Jeremy Musser <jemusser@iu.edu>

RUN apt-get update
RUN apt-get -y install python-setuptools python-pip

COPY . /unis

ADD unis/test/docker/build.sh .
RUN chmod +x build.sh
RUN ./build.sh

ADD unis/test/docker/runtests.sh .
ADD unis/test/docker/read.py .
ADD unis/test/docker/write.py .
ADD unis/test/docker/wait-for-it.sh .

RUN chmod +x runtests.sh
