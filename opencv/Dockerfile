FROM golang:1.21 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o hub hub.go

FROM alpine:3.18
WORKDIR /app
COPY --from=build /app/hub /app/hub
EXPOSE 9000
ENTRYPOINT ["/app/hub", "--addr", ":9000"]
