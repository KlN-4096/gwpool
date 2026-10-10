package types

import "time"

// RouteDescriptor contains hashes of identities and private route material,
// never tokens, proxy credentials, session IDs or raw cookies.
type RouteDescriptor struct {
	Identity string
	Exit     Exit // Requested logical exit, not a claim about the observed IP.
	Mode     string
	Source   string
	Seed     string `json:",omitempty"`
}

// RouteSource is a stable configuration fingerprint; a random SID is only a
// binding handle and must not reset route history.
type RouteSource struct {
	ID   string
	Kind string
}

type RouteAttempt struct {
	Route       RouteDescriptor
	StartedAt   time.Time
	At          time.Time
	Expected    string
	Gateway     string
	Status      int
	Failure     string
	Donor       bool
	TargetMatch string
}

type RouteState struct {
	Route          RouteDescriptor
	StartedAt      time.Time
	LastAttempt    time.Time
	Gateway        string
	ObservedAt     time.Time
	NextCheck      time.Time
	LowYieldStreak int
	LastMissed     string
	LastFailure    string
	LastTarget     string
	LastDonor      bool
	LastProductive bool
	TargetMatch    string
	Attempts       uint64
	Productive     uint64
	Drifts         uint64
	AddedCoverage  uint64
	ExtendedMS     uint64
	DonorGainMS    uint64
}

// RouteSample is diagnostic evidence only; no raw route material is included.
type RouteSample struct {
	ID        string
	Route     RouteDescriptor
	StartedAt time.Time
	Target    string `json:",omitempty"`
	TargetHit bool
	Result    string
	Gain      StockGain
	NextCheck time.Time
	Donor     bool
	// Effective deadlines measured under the atomic stock commit, enabling
	// availability accounting without reconstructing private ticket material.
	BeforeExpiresAt time.Time `json:",omitzero"`
	AfterExpiresAt  time.Time `json:",omitzero"`
}
