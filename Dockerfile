FROM alpine:3.7

RUN apk add --no-cache ca-certificates cdrkit

COPY node-update-controller /usr/local/bin
COPY webhook /usr/local/bin

USER nobody
