FROM golang:1.26-alpine AS builder
WORKDIR /build

COPY Raft/    /build/raft/
COPY Zodiac/  /build/zodiac/

WORKDIR /build/zodiac
RUN go mod edit -replace github.com/MHS-20/Raft=/build/raft && \
    go mod tidy
RUN CGO_ENABLED=0 go build -o /zodiacServer ./cmd/zodiacServer/

FROM alpine:3.20
RUN apk add --no-cache curl jq
COPY --from=builder /zodiac /zodiac
COPY Zodiac/deploy/entrypoint.sh /entrypoint.sh
COPY Zodiac/deploy/test.sh      /test.sh
RUN chmod +x /entrypoint.sh /test.sh
ENTRYPOINT ["/entrypoint.sh"]
