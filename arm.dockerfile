FROM golang:1.19 AS build

ARG VERSION=v2
ARG RELEASE_VERSION
ARG BUILD_MODE

ADD . /go/src/github.com/kubeflow/mpi-operator
WORKDIR /go/src/github.com/kubeflow/mpi-operator
RUN GOOS=linux GOARCH=arm64 go build -o _output/cmd/bin/mpi-operator.v2 ./cmd/mpi-operator
RUN ln -s mpi-operator.${VERSION} _output/cmd/bin/mpi-operator

FROM --platform=linux/arm64 gcr.io/distroless/base-debian10:latest

ENV CONTROLLER_VERSION=$VERSION
COPY --from=build /go/src/github.com/kubeflow/mpi-operator/_output/cmd/bin/* /opt/
COPY third_party/library/license.txt /opt/license.txt

ENTRYPOINT ["/opt/mpi-operator"]
CMD ["--help"]
