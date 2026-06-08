FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/better-otbr-ha-dashboard .

FROM alpine:3.20
COPY --from=build /out/better-otbr-ha-dashboard /better-otbr-ha-dashboard
COPY static /static
EXPOSE 8888
ENTRYPOINT ["/better-otbr-ha-dashboard"]
