FROM golang:1.25 AS build
WORKDIR /src
COPY . .
RUN go mod tidy
RUN CGO_ENABLED=0 go build -o /app  .
RUN ls -l


FROM gcr.io/distroless/static-debian13:nonroot
USER 1000:1000
WORKDIR /home/siuyin
COPY --from=build --chown=1000:1000 /app .
ENTRYPOINT ["./app"]
