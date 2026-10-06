package types

import "testing"

func TestUnauthorizedCredential(t *testing.T) {
	for _, tc := range []struct {
		reason string
		want   bool
	}{
		{"铸票 HTTP 401：expired", true},
		{"上游否决了这个账号（HTTP 401：expired）", true},
		{"铸票 HTTP 400：mentions 401", false},
		{"铸票 HTTP 403：blocked", false},
		{"铸票 HTTP 429：limited", false},
		{"OAuth HTTP 401：refresh failed", false},
		{"manual 401", false},
		{"", false},
	} {
		a := Account{Creds: Creds{AccessToken: "fake"}, DisabledReason: tc.reason}
		if UnauthorizedCredential(a) != tc.want {
			t.Fatalf("classification for %q", tc.reason)
		}
		a.Enabled = true
		if UnauthorizedCredential(a) {
			t.Fatal("enabled account selected")
		}
		a.Enabled = false
		a.Creds.AccessToken = ""
		if UnauthorizedCredential(a) {
			t.Fatal("external placeholder selected")
		}
	}
}
