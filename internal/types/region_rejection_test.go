package types

import "testing"

func TestUnclassified403DoesNotRejectWholeAccount(t *testing.T) {
	if RejectedByUpstream(403) {
		t.Fatal("403 can reject the exit country, not the account credential")
	}
	if !RejectedByUpstream(401) {
		t.Fatal("credential authentication failures must still reject the account")
	}
}
