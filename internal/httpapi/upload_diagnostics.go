package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"gwpool/internal/oauth"
)

type uploadRefreshError struct{ err error }

func (e *uploadRefreshError) Error() string {
	return "用 refresh_token 换 access_token 失败：" + e.err.Error()
}

func (e *uploadRefreshError) Unwrap() error { return e.err }

// Record outcome metadata only. Credentials, identity, labels, URLs, and raw
// upstream errors must not enter these diagnostics.
type uploadDiagnostics struct {
	http.ResponseWriter
	id, stage, reason string
	started           time.Time
	status, upstream  int
	writeFailed       bool
}

func newUploadDiagnostics(w http.ResponseWriter) *uploadDiagnostics {
	d := &uploadDiagnostics{ResponseWriter: w, stage: "read", started: time.Now()}
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err == nil {
		d.id = hex.EncodeToString(nonce[:])
		w.Header().Set("X-Gwpool-Upload-ID", d.id)
	}
	return d
}

func (d *uploadDiagnostics) Unwrap() http.ResponseWriter { return d.ResponseWriter }

func (d *uploadDiagnostics) WriteHeader(status int) {
	if d.status == 0 {
		d.status = status
		d.ResponseWriter.WriteHeader(status)
	}
}

func (d *uploadDiagnostics) Write(body []byte) (int, error) {
	if d.status == 0 {
		d.WriteHeader(http.StatusOK)
	}
	n, err := d.ResponseWriter.Write(body)
	d.writeFailed = d.writeFailed || err != nil || n != len(body)
	return n, err
}

func (d *uploadDiagnostics) credentialFailure(err error) {
	d.reason = "invalid_credentials"
	var refresh *uploadRefreshError
	if !errors.As(err, &refresh) {
		return
	}
	d.stage, d.reason = "refresh", "refresh_failed"
	var upstream *oauth.RefreshError
	var network net.Error
	switch {
	case errors.As(err, &upstream):
		d.reason, d.upstream = upstream.Code, upstream.Status
	case errors.Is(err, context.Canceled):
		d.reason = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		d.reason = "timeout"
	case errors.As(err, &network):
		d.reason = "network"
		if network.Timeout() {
			d.reason = "timeout"
		}
	}
}

func (d *uploadDiagnostics) finish(log *slog.Logger, ctx context.Context) {
	if d.writeFailed {
		d.reason = "response_write_failed"
	} else if ctx.Err() != nil {
		d.reason = "client_canceled"
	} else if d.reason == "" {
		d.reason = "ok"
		if d.status >= http.StatusBadRequest {
			d.reason = "rejected"
		}
	}
	log.Info("凭据上传结果", "upload_id", d.id, "stage", d.stage,
		"reason", d.reason, "status", d.status, "upstream_status", d.upstream,
		"duration_ms", time.Since(d.started).Milliseconds())
}
