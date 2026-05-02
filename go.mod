module github.com/simpletrack/analytics-service

go 1.25.0

require (
	github.com/redis/go-redis/v9 v9.19.0
	github.com/simpletrack/analytics-core v0.0.0
	github.com/valyala/fasthttp v1.70.0
)

require (
	github.com/andybalholm/brotli v1.2.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/klauspost/compress v1.18.5 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace github.com/simpletrack/analytics-core => ../analytics-core
