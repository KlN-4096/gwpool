package httpapi

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"testing"

	"gwpool/internal/oauth"
	"gwpool/internal/types"
)

type uploadDiagnosticTransport func(*http.Request) (*http.Response, error)

func (f uploadDiagnosticTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestUploadDiagnosticsClassifyRefreshFailureWithoutSecrets(t *testing.T) {
	srv, h, _, _, _ := rig(t, types.ModeSingle)
	var logs bytes.Buffer
	srv.opt.Log = slog.New(slog.NewTextHandler(&logs, nil))
	const secret = "rt-offline-sensitive-token-not-to-log"
	srv.opt.Refresh = func(ctx context.Context, rt string) (oauth.Tokens, error) {
		client := &http.Client{Transport: uploadDiagnosticTransport(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusBadRequest,
				Body: io.NopCloser(strings.NewReader(
					`{"error":"invalid_grant","error_description":"` + secret + `"}`)),
				Header: make(http.Header),
			}, nil
		})}
		return oauth.Refresh(ctx, client, rt)
	}
	w := uploadIdentity(t, h, map[string]string{"refresh_token": secret})
	if w.Code != http.StatusBadRequest {
		t.Fatal(w.Code)
	}
	requestID := w.Header().Get("X-Gwpool-Upload-ID")
	if !regexp.MustCompile(`^[a-f0-9]{16}$`).MatchString(requestID) {
		t.Fatal("missing bounded upload diagnostic ID")
	}
	for _, want := range []string{"upload_id=" + requestID, "stage=refresh", "reason=invalid_grant", "upstream_status=400", "status=400", "duration_ms="} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing diagnostic field %q", want)
		}
	}
	if strings.Contains(logs.String(), secret) || strings.Contains(w.Body.String(), secret) {
		t.Fatal("credential or upstream description leaked")
	}
}

func TestUploadDiagnosticsValidationAndCancellationAreDistinct(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		srv, h, _, _, _ := rig(t, types.ModeSingle)
		var logs bytes.Buffer
		srv.opt.Log = slog.New(slog.NewTextHandler(&logs, nil))
		secret := "short"
		reason := "invalid_credentials"
		if canceled {
			secret = "rt-offline-sensitive-token-not-to-log"
			reason = "canceled"
			srv.opt.Refresh = func(context.Context, string) (oauth.Tokens, error) {
				return oauth.Tokens{}, context.Canceled
			}
		}
		w := uploadIdentity(t, h, map[string]string{"refresh_token": secret})
		if w.Code != http.StatusBadRequest || !strings.Contains(logs.String(), "reason="+reason) {
			t.Errorf("failure category not recorded: canceled=%v status=%d", canceled, w.Code)
		}
		if strings.Contains(logs.String(), secret) {
			t.Fatal("request credential leaked")
		}
	}
}
