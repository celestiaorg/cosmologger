# FROM golang:alpine3.15 AS development
FROM golang:alpine AS development
ARG arch=x86_64

# ENV CGO_ENABLED=0
WORKDIR /go/src/app/
COPY . /go/src/app/

RUN set -eux; \
    apk add --no-cache \
    git \
    openssh \
    ca-certificates \
    build-base \
    && mkdir -p /build/ 

# We need these configs to handle the private repos
# If you do not have the my_keys directory it will be ignored 
# and will raise an error if a private repo found
# get your keys copied in the `my_keys` directory: 
#   mkdir -p ./my_keys && mkdir -p ./my_keys/.ssh
#   cp ~/.ssh/id_rsa ./my_keys/.ssh && cp ~/.gitconfig ./my_keys
# COPY . /go/src/app/
# COPY main.go my_keys/.ssh/id_rsa* /root/.ssh/
# COPY main.go my_keys/.gitconfig* /root/
# RUN chmod 700 /root/* \
#     && echo -e "\nHost github.com\n\tStrictHostKeyChecking no\n" >> /root/.ssh/config \
#     && cd ~ \
#     && git config --global url.ssh://git@github.com/.insteadOf https://github.com/


RUN go get github.com/go-delve/delve/cmd/dlv \
    && go build -mod=readonly -buildvcs=false -o /build/app . \
    && cp conf.json /build/

ENV PATH=$PATH:/build

# ENTRYPOINT [ "dlv", "debug", "--headless", "--log", "--listen=:2345", "--api-version=2"]
ENTRYPOINT ["tail", "-f", "/dev/null"]

#----------------------------#

FROM development AS test

WORKDIR /go/src/app/

ENV EXEC_PATH=/go/src/app/

ENTRYPOINT ["go", "test", "-v", "./..."]

#----------------------------#

FROM alpine:latest AS production

WORKDIR /app/
COPY --from=development /build .
RUN apk --no-cache add \
    curl 

ENTRYPOINT ["./app"]