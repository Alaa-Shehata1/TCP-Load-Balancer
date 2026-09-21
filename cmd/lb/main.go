// Command lb runs the TCP load balancer.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alaa157/tcp-load-balancer/internal/balancer"
	"github.com/alaa157/tcp-load-balancer/internal/config"
	"github.com/alaa157/tcp-load-balancer/internal/healthcheck"
	"github.com/alaa157/tcp-load-balancer/internal/metrics"
	"github.com/alaa157/tcp-load-balancer/internal/pool"
	"github.com/alaa157/tcp-load-balancer/internal/proxy"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to YAML config")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Error("load config", "err", err)
		os.Exit(1)
	}

	p := pool.New(cfg.Backends)
	b, err := balancer.New(cfg.Algorithm, p)
	if err != nil {
		log.Error("balancer", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	healthcheck.Start(ctx, p,
		time.Duration(cfg.Healthcheck.IntervalSecs)*time.Second,
		time.Duration(cfg.Healthcheck.TimeoutSecs)*time.Second,
		cfg.Healthcheck.FailThreshold)

	srv := proxy.New(p, b,
		time.Duration(cfg.Timeouts.DialSecs)*time.Second,
		time.Duration(cfg.Timeouts.IdleSecs)*time.Second, log)

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Error("listen", "addr", cfg.ListenAddr, "err", err)
		os.Exit(1)
	}
	log.Info("lb listening", "addr", cfg.ListenAddr, "algo", cfg.Algorithm)

	admin := &http.Server{Addr: cfg.AdminAddr, Handler: metrics.New(p)}
	go func() {
		log.Info("admin listening", "addr", cfg.AdminAddr)
		if err := admin.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("admin", "err", err)
		}
	}()

	go func() {
		err := srv.Serve(ln) // returns only when the listener closes
		log.Info("proxy stopped", "err", err)
	}()

	<-ctx.Done()
	log.Info("draining: stop accept")
	_ = ln.Close()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = admin.Shutdown(shutCtx)
	log.Info("bye")
}
