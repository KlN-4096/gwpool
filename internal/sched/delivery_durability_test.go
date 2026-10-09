package sched

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

var errDeliveryStorage = errors.New("injected delivery persistence failure")

type failingDeliveryStore struct{ Store }

func (f failingDeliveryStore) RecordExternalUsage(string, string, time.Time, bool, bool) error {
	return errDeliveryStorage
}

func TestExternalStatisticsFailureDoesNotPreventDelivery(t *testing.T) {
	s, _, _, _ := anonRig(t, 1, 1, 1)
	s.st = failingDeliveryStore{s.st}
	var log bytes.Buffer
	s.opt.Log = slog.New(slog.NewTextHandler(&log, nil))
	d, err := s.DeliverAnonymous(context.Background(), "member", Request{Gateway: "unified-1"}, true)
	if err != nil || d.Cookie == "" {
		t.Fatalf("statistics failure must not become a stock failure: %+v %v", d, err)
	}
	if !strings.Contains(log.String(), "external consumer statistics write failed") {
		t.Fatal("statistics failure was silently discarded")
	}
}
