FROM golang:1.5
MAINTAINER Hugo González Labrador

ENV CLAWIO_LOCALSTOREXATTRPROP_PORT 57003
#redis://:p4ssw0rd@10.0.1.1:6380/15
ENV CLAWIO_LOCALSTOREXATTRPROP_DSN "service-localstorexattr-prop-redis:6379"
ENV CLAWIO_SHAREDSECRET secret

ADD . /go/src/github.com/clawio/service.localstorexattr.propredis
WORKDIR /go/src/github.com/clawio/service.localstorexattr.propredis

RUN go get -u github.com/tools/godep
RUN godep restore
RUN go install

ENTRYPOINT /go/bin/service.localstorexattr.propredis

EXPOSE 57003

