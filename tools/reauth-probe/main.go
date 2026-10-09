//go:build reauth_probe

// A deliberately opt-in, one-account login probe. Config arrives over stdin;
// credentials stay in the user-supplied file. No tokens or identities are printed
// or saved, and nothing is written to the pool.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/mint"
	"gwpool/internal/reauth"
)

func main() {
	path := flag.String("accounts", "", "user-provided input file")
	lineNumber := flag.Int("line", 1, "one-based record line; surrounding text is never interpreted")
	provider := flag.String("provider", "", "optional configured provider name")
	flag.Parse()
	if *path == "" {
		stop("input_file_required")
	}
	var cfg config.Config
	if err := json.NewDecoder(os.Stdin).Decode(&cfg); err != nil {
		stop("invalid_config")
	}
	if *provider != "" {
		for i := range cfg.Proxies {
			cfg.Proxies[i].Enabled = cfg.Proxies[i].Enabled && cfg.Proxies[i].Name == *provider
		}
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		stop("cannot_read_input")
	}
	var input reauth.Input
	for i, line := range strings.Split(strings.TrimPrefix(string(data), "\ufeff"), "\n") {
		if i+1 != *lineNumber {
			continue
		}
		line = strings.TrimSuffix(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		parts := strings.Split(line, "----")
		if len(parts) < 2 || len(parts) > 3 {
			stop("invalid_record_shape")
		}
		input.Email, input.Password = parts[0], parts[1]
		if len(parts) == 3 {
			input.TOTP = parts[2]
		}
		break
	}
	clear(data)
	if err := input.Validate(); err != nil {
		stop("invalid_login_input")
	}
	transport, err := mint.LoginTransport(cfg)
	if err != nil {
		stop("no_eligible_dynamic_proxy")
	}
	client, err := reauth.New(transport)
	if err != nil {
		stop("cannot_create_session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	start := time.Now()
	tokens, err := client.Login(ctx, input, reauth.Hooks{Progress: func(stage string) {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"stage": stage, "elapsed_ms": time.Since(start).Milliseconds()})
	}})
	if err != nil {
		// reauth errors contain only fixed codes and HTTP status.
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"elapsed_ms": time.Since(start).Milliseconds()})
		stop(err.Error())
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"success": true, "has_access": tokens.AccessToken != "", "has_refresh": tokens.RefreshToken != "",
		"elapsed_ms": time.Since(start).Milliseconds(), "pool_writes": 0,
	})
}

func stop(code string) {
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"success": false, "reason": code, "pool_writes": 0})
	fmt.Fprintln(os.Stderr, "probe stopped; no automatic retry")
	os.Exit(1)
}
