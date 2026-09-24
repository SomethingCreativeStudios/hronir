# syntax=docker/dockerfile:1.7
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath -ldflags="-s -w -X main.version=${VERSION}" -o /out/hronir ./cmd/hronir

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hronir /hronir
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/hronir"]
CMD ["run"]
