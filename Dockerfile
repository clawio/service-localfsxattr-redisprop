FROM golang:1.5
MAINTAINER Hugo González Labrador

ENV CLAWIO_LOCALFSXATTR_REDISPROP_PORT 57023
# redis://:p4ssw0rd@10.0.1.1:6380/15
ENV CLAWIO_LOCALFSXATTR_REDISPROP_LOGLEVEL "error"
ENV CLAWIO_LOCALFSXATTR_REDISPROP_DSN "service-localfsxattr-redisprop-redis:6379"
ENV CLAWIO_LOCALFSXATTR_REDISPROP_MAXREDISIDLE 1024
ENV CLAWIO_LOCALFSXATTR_REDISPROP_MAXREDISACTIVE 1024
ENV CLAWIO_SHAREDSECRET secret

ADD . /go/src/github.com/clawio/service-localfsxattr-redisprop
WORKDIR /go/src/github.com/clawio/service-localfsxattr-redisprop

RUN go get -u github.com/tools/godep
RUN godep restore
RUN go install

ENTRYPOINT /go/bin/service-localfsxattr-redisprop

EXPOSE 57023

