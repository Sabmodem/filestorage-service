# Stage 1: Build the Go application
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker cache
COPY go.mod go.sum ./

# Download Go modules
RUN go mod download

# Copy the rest of the application source code
COPY . .

# Generate Swagger documentation
# This step needs to run *before* the build, so the docs are available.
# Ensure 'swag' is available in the builder image.
RUN go install github.com/swaggo/swag/cmd/swag@latest
RUN swag init

# Run go mod tidy again after swag init to ensure go.mod/go.sum are up-to-date
# with any dependencies introduced by swag init.
RUN go mod tidy

# Build the Go application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -ldflags "-s -w" -o /app/file-service .

# Stage 2: Create the final, minimal image
FROM alpine:latest

# Set the working directory
WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /app/file-service .
# Copy the generated docs folder from the builder stage
COPY --from=builder /app/docs ./docs

# Expose the port your Go application listens on
EXPOSE 8080

# Command to run the application
ENTRYPOINT ["/app/file-service"]
