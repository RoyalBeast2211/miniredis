# ==============================================================================
# Multi-stage Dockerfile for MiniRedis
# ==============================================================================

# Stage 1: Build static Go binary
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Install build essentials and git
RUN apk add --no-cache ca-certificates tzdata

# Cache Go dependencies
COPY go.mod ./
# RUN go mod download (if external dependencies were present)

# Copy complete source code
COPY . .

# Compile static, stripped binary with zero external C dependencies
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -extldflags '-static'" \
    -trimpath \
    -o /bin/miniredis .

# Stage 2: Minimal, secure runtime container running as unprivileged user
FROM alpine:3.21

# Install runtime dependencies for health checks and timezone
RUN apk add --no-cache ca-certificates tzdata

# Create dedicated unprivileged non-root user and group
RUN addgroup -g 10001 miniredis && \
    adduser -u 10001 -G miniredis -D -h /data -s /sbin/nologin miniredis

# Create data and config directories owned by miniredis user
RUN mkdir -p /data /etc/miniredis && \
    chown -R miniredis:miniredis /data /etc/miniredis

# Copy binary from builder stage
COPY --from=builder /bin/miniredis /usr/local/bin/miniredis

# Copy default configuration file
COPY miniredis.conf /etc/miniredis/miniredis.conf
RUN chown miniredis:miniredis /etc/miniredis/miniredis.conf

# Run as unprivileged non-root user
USER miniredis
WORKDIR /data

# Expose standard Redis port (6379) and HTTP health check port (8080)
EXPOSE 6379 8080

# Persistent storage volume
VOLUME ["/data"]

# Default entrypoint and command
ENTRYPOINT ["/usr/local/bin/miniredis"]
CMD ["-config", "/etc/miniredis/miniredis.conf", "-aof", "/data/appendonly.aof", "-snapshot", "/data/dump.rdb"]
