package sched

import (
	"context"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/routing"
	"gwpool/internal/types"
)

type routingObservationSource interface {
	RoutingObservations(time.Time) (routing.History, error)
}

const inventoryForecastCacheLimit = 4096

type inventoryRoutingRound struct {
	model       *routing.Model
	predictions map[routingCandidate]types.InventoryPrediction
}

type routingCandidate struct {
	donor string
	exit  types.Exit
}

// Called once before maintenance workers start. The frozen model cannot learn
// from the outcome it is about to predict, including other workers in this round.
func (s *Scheduler) newInventoryRoutingRound() *inventoryRoutingRound {
	if s.cfg().InventoryRoutingMode() == config.InventoryRoutingOff {
		return nil
	}
	var snapshot routing.ModelSnapshot
	var err error
	if source, ok := s.st.(interface {
		RoutingModel(time.Time) (routing.ModelSnapshot, error)
	}); ok {
		snapshot, err = source.RoutingModel(s.now())
	} else if source, ok := s.st.(routingObservationSource); ok {
		var history routing.History
		history, err = source.RoutingObservations(s.now())
		snapshot = routing.ModelSnapshot{Model: routing.New(history.Observations),
			Loaded: history.Loaded, Clipped: history.Clipped, SkippedLines: history.SkippedLines}
	} else {
		return nil
	}
	if err != nil {
		s.opt.Log.Warn("库存选路历史不可用，保留原选择", "err", err)
		return nil
	}
	if snapshot.Loaded && (snapshot.Clipped || snapshot.SkippedLines > 0) {
		s.opt.Log.Warn("库存选路仅使用有界可读历史",
			"tail_clipped", snapshot.Clipped, "skipped_lines", snapshot.SkippedLines)
	}
	return &inventoryRoutingRound{model: snapshot.Model,
		predictions: map[routingCandidate]types.InventoryPrediction{}}
}

// Caller holds premintRound.mu; the cache is shared by the round's workers.
func (r *inventoryRoutingRound) forecast(account types.Account, region types.Region) types.InventoryPrediction {
	key := routingCandidate{donor: routing.DonorKey(account.Creds.AccountID), exit: region.Exit}
	if p, ok := r.predictions[key]; ok {
		return p
	}
	p := r.model.ForExit(key.donor, key.exit)
	if p.Gateway != "" && len(r.predictions) < inventoryForecastCacheLimit {
		r.predictions[key] = p
	}
	return p
}

const (
	inventoryPredictedLive    = 0
	inventoryPredictionNone   = 1
	inventoryPredictedMissing = 2
)

func inventoryPredictionRank(prediction types.InventoryPrediction, known map[string]types.Gateway,
	live, aimed map[string]bool) int {
	if prediction.Gateway == "" {
		return inventoryPredictionNone
	}
	if _, ok := known[prediction.Gateway]; !ok {
		return inventoryPredictionNone
	}
	if live[prediction.Gateway] || aimed[prediction.Gateway] {
		return inventoryPredictedLive
	}
	return inventoryPredictedMissing
}

type inventoryPredictionContextKey struct{}

type inventoryPredictionContext struct {
	account    int64
	exit       types.Exit
	prediction types.InventoryPrediction
}

func withInventoryPrediction(ctx context.Context, account int64, exit types.Exit,
	prediction *types.InventoryPrediction) context.Context {
	if prediction == nil {
		return ctx
	}
	return context.WithValue(ctx, inventoryPredictionContextKey{},
		inventoryPredictionContext{account: account, exit: exit, prediction: *prediction})
}

func inventoryPredictionFromContext(ctx context.Context, account int64, exit types.Exit) *types.InventoryPrediction {
	value, ok := ctx.Value(inventoryPredictionContextKey{}).(inventoryPredictionContext)
	if !ok || value.account != account || value.exit != exit {
		return nil
	}
	prediction := value.prediction
	return &prediction
}
