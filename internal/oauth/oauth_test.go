package oauth

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRefreshPreservesIDToken(t *testing.T) {
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.String() != tokenURL {
			t.Fatal("unexpected token endpoint request")
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"access_token":"test-access","refresh_token":"test-refresh","id_token":"test-id"}`)),
		}, nil
	})}
	tokens, err := Refresh(context.Background(), hc, "test-refresh-input")
	if err != nil || tokens.AccessToken != "test-access" ||
		tokens.RefreshToken != "test-refresh" || tokens.IDToken != "test-id" {
		t.Fatalf("token response fields not preserved: %v", err)
	}
}

func TestRefreshErrorDoesNotEchoSensitiveBody(t *testing.T) {
	hc := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"error":"invalid_grant","error_description":"fake-secret-must-not-leak","refresh_token":"fake-secret-must-not-leak"}`)),
		}, nil
	})}
	_, err := Refresh(context.Background(), hc, "fake-input")
	if err == nil || strings.Contains(err.Error(), "fake-secret-must-not-leak") ||
		!strings.Contains(err.Error(), "400") || !strings.Contains(err.Error(), "invalid_grant") {
		t.Fatal("刷新错误必须只保留状态码和白名单类别，不得回显响应体")
	}
}
