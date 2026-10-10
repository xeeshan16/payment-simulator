FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /paysim ./cmd/paysim

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /paysim /paysim
EXPOSE 8080
ENTRYPOINT ["/paysim"]
