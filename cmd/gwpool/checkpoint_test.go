package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/store"
	"gwpool/internal/types"
)

func TestCheckpointOnlyPublishesAcknowledgedJournal(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := st.PutSlot(types.Slot{AccountID: 77, Gateway: "g", LastDeliver: now}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.DataDir = dir
	cfg.Proxies = []config.Proxy{{Name: "offline", Kind: config.KindNovproxy,
		Host: "127.0.0.1:1", User: "fixture", Pass: "fixture", Enabled: true}}
	configPath := filepath.Join(dir, "config.json")
	body, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, body, 0600); err != nil {
		t.Fatal(err)
	}
	readSnapshot := func() store.State {
		t.Helper()
		body, err := os.ReadFile(filepath.Join(dir, "state.json"))
		if err != nil {
			t.Fatal(err)
		}
		var state store.State
		if err := json.Unmarshal(body, &state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	if !readSnapshot().Slots[store.SlotKey(77, "g")].LastDeliver.IsZero() {
		t.Fatal("fixture must require WAL replay")
	}
	if err := checkpointOnlyRun(configPath, nil); err != nil {
		t.Fatal(err)
	}
	if !readSnapshot().Slots[store.SlotKey(77, "g")].LastDeliver.Equal(now) {
		t.Fatal("snapshot-only rollback would lose acknowledged delivery")
	}
}
