// Package config loads and validates YAML for the LB.
package config

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
)

// BackendConfig describes one upstream TCP server.
type BackendConfig struct {
	Name string `yaml:"name"`
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	Addr string `yaml:"-"`
}

// HCConfig controls active health checking.
type HCConfig struct {
	IntervalSecs  int `yaml:"interval_secs"`
	TimeoutSecs   int `yaml:"timeout_secs"`
	FailThreshold int `yaml:"fail_threshold"`
}

// TimeoutConfig controls dial and idle timeouts.
type TimeoutConfig struct {
	DialSecs int `yaml:"dial_secs"`
	IdleSecs int `yaml:"idle_secs"`
}

// Config is the load balancer configuration.
type Config struct {
	ListenAddr  string          `yaml:"listen_addr"`
	AdminAddr   string          `yaml:"admin_addr"`
	Algorithm   string          `yaml:"algorithm"`
	Backends    []BackendConfig `yaml:"backends"`
	Healthcheck HCConfig        `yaml:"healthcheck"`
	Timeouts    TimeoutConfig   `yaml:"timeouts"`
}

// Load reads, parses, and validates a YAML config file.
func Load(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse config: %w", err)
	}
	if c.Algorithm != "round-robin" && c.Algorithm != "least-conn" {
		return c, fmt.Errorf("unknown algorithm %q", c.Algorithm)
	}
	if len(c.Backends) == 0 {
		return c, fmt.Errorf("no backends")
	}
	for i := range c.Backends {
		bc := &c.Backends[i]
		if bc.Port <= 0 || bc.Port > 65535 {
			return c, fmt.Errorf("backend %s bad port %d", bc.Name, bc.Port)
		}
		if bc.Host == "" {
			return c, fmt.Errorf("backend %s missing host", bc.Name)
		}
		bc.Addr = net.JoinHostPort(bc.Host, fmt.Sprint(bc.Port))
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":9000"
	}
	if c.AdminAddr == "" {
		c.AdminAddr = ":8080"
	}
	if c.Healthcheck.IntervalSecs == 0 {
		c.Healthcheck.IntervalSecs = 5
	}
	if c.Healthcheck.TimeoutSecs == 0 {
		c.Healthcheck.TimeoutSecs = 2
	}
	if c.Healthcheck.FailThreshold == 0 {
		c.Healthcheck.FailThreshold = 2
	}
	if c.Timeouts.DialSecs == 0 {
		c.Timeouts.DialSecs = 3
	}
	if c.Timeouts.IdleSecs == 0 {
		c.Timeouts.IdleSecs = 60
	}
	return c, nil
}
