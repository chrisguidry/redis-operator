# The operator's image is one static Go binary. The operator holds
# cluster credentials and the Redis passwords, so the image has no shell,
# no libc, and no tools.

FROM golang:1.27.2-bookworm AS build
WORKDIR /src
# The module files come first, so a source change reuses the cached
# download layer.
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
ARG VERSION=dev
# CGO_ENABLED=0 makes a static binary that runs without a loader.
# -trimpath removes the build machine's paths from it.
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /redis-operator .

FROM scratch
COPY --from=build /redis-operator /redis-operator
USER 65532:65532
ENTRYPOINT ["/redis-operator"]
