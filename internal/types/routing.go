package types

import "time"

// InventoryPrediction was frozen before an inventory attempt. Country is a
// historical estimate, not the current Sample.Observed.Country. Gateway may be
// empty when the model abstains; the failed/unknown attempt is still recorded.
type InventoryPrediction struct {
	Model           string
	Mode            string
	Gateway         string
	Country         string
	GeoBasis        string
	TrainingSamples int
	DonorSamples    int
	CountrySamples  int
	ExitSamples     int
	TrainedThrough  time.Time
	DecidedAt       time.Time
}
