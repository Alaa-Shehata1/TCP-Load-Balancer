FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /lb ./cmd/lb

FROM alpine:3.20
COPY --from=build /lb /lb
COPY config.docker.yaml /config.yaml
EXPOSE 9000 8080
ENTRYPOINT ["/lb", "--config", "/config.yaml"]
