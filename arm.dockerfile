FROM golang:1.19 AS build

ARG VERSION=v2
ARG RELEASE_VERSION
ARG BUILD_MODE

ADD . /go/src/github.com/kuizhiqing/resilient-training-operator
WORKDIR /go/src/github.com/kuizhiqing/resilient-training-operator
RUN GOOS=linux GOARCH=arm64 go build -o _output/cmd/bin/resilient-training-operator.v2 ./cmd/resilient-training-operator
RUN ln -s resilient-training-operator.${VERSION} _output/cmd/bin/resilient-training-operator

FROM --platform=linux/arm64 gcr.io/distroless/base-debian10:latest

ENV CONTROLLER_VERSION=$VERSION
COPY --from=build /go/src/github.com/kuizhiqing/resilient-training-operator/_output/cmd/bin/* /opt/
COPY third_party/library/license.txt /opt/license.txt

ENTRYPOINT ["/opt/resilient-training-operator"]
CMD ["--help"]
