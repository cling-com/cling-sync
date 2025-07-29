//nolint:forbidigo
package http

import (
	"log/slog"
	"net/http"
	"os"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	statusCode int
	size       int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK // Default to 200 if WriteHeader wasn't called
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.size += n
	return n, err //nolint:wrapcheck
}

func RequestLogMiddleware(handler http.Handler) http.Handler {
	// todo: Make the log format and level configurable
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})) //nolint:exhaustruct
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t0 := time.Now()
		wrapped := &responseWriter{w, 0, 0}
		handler.ServeHTTP(wrapped, r)
		log.Debug(
			"HTTP request",
			"status",
			wrapped.statusCode,
			"method",
			r.Method,
			"path",
			r.URL.Path,
			"remote",
			r.RemoteAddr,
			"duration",
			time.Since(t0),
			"request_size",
			r.ContentLength,
			"response_size",
			wrapped.size,
		)
	})
}

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			// This is a CORS preflight request, we don't need to do anything.
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
