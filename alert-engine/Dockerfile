# ─── Build ──────────────────────────────────────────────────────────────────
FROM golang:1.25-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/alert-engine .

# ─── Runtime ────────────────────────────────────────────────────────────────
FROM alpine:3.20
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 engine
USER engine
COPY --from=build /out/alert-engine /usr/local/bin/alert-engine

ENTRYPOINT ["alert-engine"]
