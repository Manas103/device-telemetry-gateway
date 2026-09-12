# Container shape for cmd/deliverygateway. Authored to describe how this
# service is meant to run; not built or run in this session (Docker was
# unavailable in this environment, see README). Multi-stage: compile the Go
# binary in a build stage, run it in a minimal runtime image.
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/deliverygateway ./cmd/deliverygateway

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/deliverygateway /deliverygateway
EXPOSE 8090
ENTRYPOINT ["/deliverygateway", "-addr=0.0.0.0:8090"]
