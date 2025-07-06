package http

import (
	"log/slog"
	"net/http"
	"os"
)

func RequestLogHander(handler http.HandlerFunc) http.HandlerFunc {
	// todo: We should not use a logger per route
	// todo: Make the log format and level configurable
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})) //nolint:exhaustruct
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Info("HTTP request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		handler(w, r)
	})
}
