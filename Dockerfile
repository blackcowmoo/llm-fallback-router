# syntax=docker/dockerfile:1

ARG GO_VERSION=1.24
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w \
      -X github.com/blackcowmoo/llm-fallback-router/internal/version.Version=${VERSION} \
      -X github.com/blackcowmoo/llm-fallback-router/internal/version.Commit=${COMMIT} \
      -X github.com/blackcowmoo/llm-fallback-router/internal/version.BuildDate=${BUILD_DATE}" \
    -o /out/llm-router ./cmd/llm-router

RUN mkdir -p /out/config \
    && cp /src/examples/router.yaml /out/config/router.yaml \
    && chmod 0755 /out/config \
    && chmod 0644 /out/config/router.yaml

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/llm-router /usr/local/bin/llm-router
COPY --from=build /out/config /etc/llm-router

USER nonroot:nonroot
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/llm-router", "healthcheck", "--url", "http://127.0.0.1:8080/healthz"]

ENTRYPOINT ["/usr/local/bin/llm-router"]
CMD ["--config", "/etc/llm-router/router.yaml"]
