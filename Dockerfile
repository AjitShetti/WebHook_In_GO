FROM golang:1.26.8-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/service ./cmd/${SERVICE}

FROM alpine:3.24

RUN apk add --no-cache ca-certificates
COPY --from=build /out/service /service
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/service"]
