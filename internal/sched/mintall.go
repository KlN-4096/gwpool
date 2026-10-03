package sched

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"gwpool/internal/types"
)

const (
	mintAllJobTimeout = time.Minute
	mintAllRetryGap   = time.Second
	mintAllRecent     = 20
	mintAllProblems   = 200
)

// MintAllResult 是管理员可见的白名单结果，不含凭据、cookie 或上游原始错误。
type MintAllResult struct {
	AccountID int64
	Region    string
	Gateway   string
	Status    string
	Reason    string
	attempted bool
}

type MintAllStatus struct {
	Active                     bool
	Total, Queued, Running     int
	Succeeded, Failed, Skipped int
	Started, Finished          time.Time
	SkipReasons                map[string]int
	Recent                     []MintAllResult
	Problems                   []MintAllResult // 本轮前 200 个异常，成功不会挤掉入队时的跳过原因。
}

type mintAllJob struct {
	accountID int64
	region    types.Region
}

// MintAllQueue 只存本轮的账号 ID 和区域，不保存凭据快照。
// Run 由服务生命周期持有；HTTP 入队后立即返回，不随浏览器断开而取消。
type MintAllQueue struct {
	pool *Scheduler
	mu   sync.Mutex
	jobs []mintAllJob
	stat MintAllStatus
	wake chan struct{}
}

func NewMintAllQueue(pool *Scheduler) *MintAllQueue {
	return &MintAllQueue{pool: pool, wake: make(chan struct{}, 1)}
}

// Enqueue 同轮幂等。按区域交错账号，避免连续九发都排给同一个号。
// 配额已经用完/上游按住的组合当场跳过；不挂到下一个小时等配额。
func (q *MintAllQueue) Enqueue() MintAllStatus {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stat.Active {
		return q.snapshot()
	}
	q.jobs = nil
	q.stat = MintAllStatus{Started: q.pool.now(), SkipReasons: map[string]int{}}
	accounts := q.pool.usable()
	sort.Slice(accounts, func(i, j int) bool { return accounts[i].ID < accounts[j].ID })
	allowed := make(map[int64]map[string]bool, len(accounts))
	for _, a := range accounts {
		used, limit := q.pool.mint.Rate(a.Creds.AccountID)
		held := q.pool.accountHeldFor(a.ID) > 0
		allowed[a.ID] = map[string]bool{}
		for _, r := range types.Regions {
			q.stat.Total++
			reason := ""
			switch {
			case held:
				reason = "上游限流"
			case limit > 0 && used[r.Key] >= limit:
				reason = "区域配额已满"
			default:
				allowed[a.ID][r.Key] = true
			}
			if reason != "" {
				q.recordResult(MintAllResult{AccountID: a.ID, Region: r.Key, Status: "skipped", Reason: reason})
			}
		}
	}
	for _, r := range types.Regions {
		for _, a := range accounts {
			if allowed[a.ID][r.Key] {
				q.jobs = append(q.jobs, mintAllJob{accountID: a.ID, region: r})
			}
		}
	}
	q.stat.Queued = len(q.jobs)
	q.stat.Active = len(q.jobs) > 0
	if !q.stat.Active {
		q.stat.Finished = q.pool.now()
	} else {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
	return q.snapshot()
}

func (q *MintAllQueue) Status() MintAllStatus {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.snapshot()
}

// 以下统计辅助在 q.mu 下调用；给页面的一律是独立副本。
func (q *MintAllQueue) snapshot() MintAllStatus {
	out := q.stat
	out.Recent = append([]MintAllResult(nil), q.stat.Recent...)
	out.Problems = append([]MintAllResult(nil), q.stat.Problems...)
	out.SkipReasons = make(map[string]int, len(q.stat.SkipReasons))
	for k, n := range q.stat.SkipReasons {
		out.SkipReasons[k] = n
	}
	return out
}

func (q *MintAllQueue) skip(reason string) {
	q.stat.Skipped++
	q.stat.SkipReasons[reason]++
}

func (q *MintAllQueue) Run(ctx context.Context) {
	defer q.cancelPending()
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.wake:
			for ctx.Err() == nil {
				job, ready, delay := q.next()
				if !ready {
					if delay == 0 {
						break
					}
					if !waitGap(ctx, delay) {
						return
					}
					continue
				}
				result, retry := q.execute(ctx, job)
				q.note(job, result, retry)
				var gap time.Duration
				if result.attempted {
					gap = q.pool.opt.MintGap
				}
				if retry {
					gap = max(gap, mintAllRetryGap)
				}
				if !waitGap(ctx, gap) {
					return
				}
			}
		}
	}
}

// 短暂的账号间隔不丢任务；先找别的账号，全都在间隔中才睡到最早可用时刻。
func (q *MintAllQueue) next() (mintAllJob, bool, time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.jobs) == 0 {
		return mintAllJob{}, false, 0
	}
	now := q.pool.now()
	q.pool.mu.Lock()
	defer q.pool.mu.Unlock()
	var earliest time.Duration
	for i, job := range q.jobs {
		left := q.pool.opt.RenewGap - now.Sub(q.pool.lastMintedAt[job.accountID])
		if left > 0 {
			if earliest == 0 || left < earliest {
				earliest = left
			}
			continue
		}
		q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
		q.stat.Queued--
		q.stat.Running = 1
		return job, true, 0
	}
	return mintAllJob{}, false, earliest
}

func (q *MintAllQueue) execute(ctx context.Context, job mintAllJob) (MintAllResult, bool) {
	s := q.pool
	// 与自动续票/补铸串行，不给已配置的后台并发数额外加一个 worker。
	s.backgroundMu.Lock()
	defer s.backgroundMu.Unlock()
	result := MintAllResult{AccountID: job.accountID, Region: job.region.Key, Status: "skipped"}
	if ctx.Err() != nil {
		result.Reason = "服务停止"
		return result, false
	}
	release, free := s.claimRegion(job.accountID, job.region.Key)
	if !free {
		return result, true
	}
	defer release()
	unlock := s.st.LockAccount(job.accountID)
	defer unlock()
	a, ok := s.account(job.accountID)
	if !ok {
		result.Reason = "账号停用、删除或缺少凭据"
		return result, false
	}
	if s.accountHeldFor(a.ID) > 0 {
		result.Reason = "上游限流"
		return result, false
	}
	used, limit := s.mint.Rate(a.Creds.AccountID)
	if limit > 0 && used[job.region.Key] >= limit {
		result.Reason = "区域配额已满"
		return result, false
	}
	if !s.claimAccount(a.ID, s.opt.RenewGap) {
		return result, true
	}
	sid, seed, err := s.premintSID(job.region.Key)
	if err != nil {
		result.Reason = "无可用出口"
		return result, false
	}
	ctx, cancel := context.WithTimeout(ctx, mintAllJobTimeout)
	defer cancel()
	result.attempted = true
	got, err := s.mintOnce(ctx, a, job.region.Exit, sid, seed, "", "")
	switch {
	case err == nil:
		result.Status, result.Gateway = "succeeded", got.pair.Gateway
	case errors.Is(err, types.ErrRateLimited):
		result.Reason = "区域配额已满或上游限流"
	case errors.Is(err, types.ErrNoExit):
		result.Reason, result.attempted = "无可用出口", false
	case errors.Is(err, ErrUpstreamRejected):
		result.Status, result.Reason = "failed", "上游拒绝账号"
	default:
		result.Status, result.Reason = "failed", "铸票失败（查看服务日志）"
	}
	return result, false
}

func (q *MintAllQueue) note(job mintAllJob, result MintAllResult, retry bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.stat.Running = 0
	if retry {
		q.jobs = append(q.jobs, job)
		q.stat.Queued++
		return
	}
	q.recordResult(result)
	if len(q.jobs) == 0 {
		q.stat.Active = false
		q.stat.Finished = q.pool.now()
	}
}

func (q *MintAllQueue) recordResult(result MintAllResult) {
	switch result.Status {
	case "succeeded":
		q.stat.Succeeded++
	case "failed":
		q.stat.Failed++
	default:
		q.skip(result.Reason)
	}
	q.stat.Recent = append([]MintAllResult{result}, q.stat.Recent...)
	if len(q.stat.Recent) > mintAllRecent {
		q.stat.Recent = q.stat.Recent[:mintAllRecent]
	}
	if result.Status != "succeeded" && len(q.stat.Problems) < mintAllProblems {
		q.stat.Problems = append(q.stat.Problems, result)
	}
}

func (q *MintAllQueue) cancelPending() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.stat.Active {
		return
	}
	for _, job := range q.jobs {
		q.recordResult(MintAllResult{AccountID: job.accountID, Region: job.region.Key, Status: "skipped", Reason: "服务停止"})
	}
	q.jobs = nil
	q.stat.Queued, q.stat.Running, q.stat.Active = 0, 0, false
	q.stat.Finished = q.pool.now()
}
