FROM golang:1.19 AS build

# Set resilient-training-operator version
# Defaults to v2
ARG VERSION=v2
ARG RELEASE_VERSION
ARG BUILD_MODE

ADD . /go/src/github.com/kuizhiqing/resilient-training-operator
WORKDIR /go/src/github.com/kuizhiqing/resilient-training-operator
# RUN make RELEASE_VERSION=${RELEASE_VERSION} ${BUILD_MODE}
RUN make RELEASE_VERSION=${RELEASE_VERSION} resilient-training-operator.v2
RUN ln -s resilient-training-operator.${VERSION} _output/cmd/bin/resilient-training-operator

FROM gcr.io/distroless/base-debian10:latest

ENV CONTROLLER_VERSION=$VERSION
COPY --from=build /go/src/github.com/kuizhiqing/resilient-training-operator/_output/cmd/bin/* /opt/
COPY third_party/library/license.txt /opt/license.txt

ENTRYPOINT ["/opt/resilient-training-operator"]
CMD ["--help"]
