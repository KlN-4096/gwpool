package types

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// ExternalConsumerUsage is telemetry, never a source of routing or admission.
// Only a digest of the caller's declared member identity is stored here.
type ExternalConsumerUsage struct {
	FirstAt     time.Time `json:"first_at"`
	LastAt      time.Time `json:"last_at"`
	Requests    uint64    `json:"requests"`
	Delivered   uint64    `json:"delivered"`
	Limited     uint64    `json:"limited"`
	LastGateway string    `json:"last_gateway,omitempty"`
}

func ExternalConsumerID(member string) string {
	sum := sha256.Sum256([]byte("gwpool-external-consumer-v1\x00" + member))
	return hex.EncodeToString(sum[:])
}
