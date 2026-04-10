FROM golang:1.21-alpine

WORKDIR /app

# 为了避免每次代码变更时重新下载依赖包，先进行 Go Modules 的缓存分离
COPY go.mod ./
RUN go mod download || true

COPY . .
