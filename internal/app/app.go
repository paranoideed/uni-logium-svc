package app

import (
	"log/slog"
	"os"
	"strings"

	"github.com/paranoideed/uni-logium-svc/internal/config"
)

type App struct {
	config *config.Config
	log    *slog.Logger
}

func New(cfg *config.Config) *App {
	a := &App{config: cfg}
	a.log = a.buildLogger()
	return a
}

func (a *App) Logger() *slog.Logger {
	return a.log
}

func (a *App) buildLogger() *slog.Logger {
	lvl := slog.LevelInfo
	switch strings.ToLower(strings.TrimSpace(a.config.Log.Level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}

	switch strings.ToLower(strings.TrimSpace(a.config.Log.Format)) {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	default:
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	}
}
