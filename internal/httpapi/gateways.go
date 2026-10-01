package httpapi

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"gwpool/internal/types"
)

// GET /gateways 报网关清单，让客户端自己挑一个它没烧过的，再用 /cookie?gateway=<名字> 去取。
//
// 为什么要把这个决定让出去：满血窗口是 (上游账号 × 网关) 的，而「这一张票接下来给哪个
// 账号用、要打几发」只有客户端自己知道。池子替它挑永远是在猜。
//
// 鉴权和 /cookie 完全一致，连 account 参数的含义也一致：返回的 used_by_you 是**那个
// 上游账号的**槽位历史，不带 account 就按这把 key 的上传者算。
func (s *Server) handleGateways(w http.ResponseWriter, r *http.Request) {
	upstream, acctID, ok := s.whoAsks(w, r)
	if !ok {
		return
	}

	now := time.Now()
	pairs := s.st.Pairs()
	// 按网关名收一份这个账号自己的槽位：槽位表的键格式归 store 所有，
	// 这里只认 Slot 自己带的两个字段，免得为一个拼键函数把 store 拖进来。
	mine := map[string]types.Slot{}
	if acctID != 0 {
		for _, sl := range s.st.Slots() {
			if sl.AccountID == acctID {
				mine[sl.Gateway] = sl
			}
		}
	}

	type row struct {
		Name string `json:"name"`
		// PairReady = 有活 pair 且剩余够交付。false 意味着你指定它的话池子得现铸。
		PairReady bool `json:"pair_ready"`
		ValidForS int  `json:"valid_for_s"`
		// 刻意没有 leased：一张 pair 同时给几个号用是允许的（见 types.Pair 的注释），
		// 「被别人占着」这个状态不存在了。
		UsedByYou  bool   `json:"used_by_you"`
		LastUsedAt string `json:"last_used_at,omitempty"`
	}
	out := struct {
		Account  string `json:"account"`
		Live     int    `json:"live"`
		Target   int    `json:"target"`
		Gateways []row  `json:"gateways"`
	}{Account: upstream, Target: s.pool.PairTarget()}

	for name := range s.st.Gateways() {
		p, hasPair := pairs[name]
		left := p.ExpiresAt.Sub(now)
		ready := hasPair && left >= types.DeliverFloor
		if ready {
			out.Live++
		}
		it := row{Name: name, PairReady: ready}
		if ready {
			it.ValidForS = int(left.Seconds())
		}
		if sl, ok := mine[name]; ok && !sl.LastTouch.IsZero() {
			// 按 LastTouch 算，**不按 LastVerdict**：判定只有上传者那条交付路径写得出来，
			// 跨账号和匿名的交付一个判定都不落 ⇒ 按判定算的话外部消费者看到的永远是
			// 「全都没用过」，照这张清单挑等于没挑。LastTouch 两条路都写，扫描和铸票
			// 也写——那些同样烧了这个账号在这个网关上的窗口。
			//
			// 还要卡 SlotCooldown：烧过要出冷却才重新给满血，三天前那一次与现在无关。
			// 不卡的话用久了每个网关都是「用过」，客户端永远挑不出落点。
			it.UsedByYou = now.Sub(sl.LastTouch) < types.SlotCooldown
			it.LastUsedAt = sl.LastTouch.UTC().Format(time.RFC3339)
		}
		out.Gateways = append(out.Gateways, it)
	}
	// 稳定顺序：客户端按这个列表挑，每次换序会让它的选择无端抖动。
	sort.Slice(out.Gateways, func(i, j int) bool { return out.Gateways[i].Name < out.Gateways[j].Name })

	writeJSON(w, http.StatusOK, out)
}

// whoAsks 认出「这次是替哪个**上游账号**问」，连它的池内行一起定出来，并把该拒的都拒掉。
//
// 一把 key 可以替多个上游账号取票：上传一份凭据换一把 key，然后用它给自己手上
// 好几个号取 pair。那种情形下槽位必须按**报上来的那个账号**记——A 烧掉一个网关
// 和 B 在同一个网关上是不是满血毫无关系，记在上传者头上会把 B 的好落点白白划掉。
//
// 行的选法必须和 /cookie **逐条对齐**（见 handleCookie）：同一个请求参数在那边写哪一行，
// 这边就得读哪一行。对不齐的后果是静默的——used_by_you 恒为 false，客户端照着一张全是
// 「没用过」的清单挑，等于没挑。
func (s *Server) whoAsks(w http.ResponseWriter, r *http.Request) (string, int64, bool) {
	ref := strings.TrimSpace(r.URL.Query().Get("account"))
	if len(ref) > 128 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "account 太长"})
		return "", 0, false
	}

	if c, ok := s.resolveCaller(r); ok {
		if c.admin {
			// 管理员没有自己的账号：槽位是 (账号 × 网关)，猜一个就等于把 A 的历史报成 B 的。
			if ref == "" {
				writeJSON(w, http.StatusBadRequest, map[string]string{
					"error": "管理员调用必须带 account 参数指明替谁"})
				return "", 0, false
			}
			// 和 /cookie 一样按托管行解析：管理员那条路取的是那一行自己的槽位。
			a, ok := s.findAccount(ref)
			if !ok {
				writeJSON(w, http.StatusNotFound, map[string]string{"error": "没有这个账号"})
				return "", 0, false
			}
			return ref, a.ID, true
		}
		if ref != "" && !accountRefMatches(c.acct, ref) {
			// 跨账号：/cookie 把这种票算在占位行上，这边也只能读那一行。
			return ref, s.rowFor(types.AnonPrefix + ref), true
		}
		return c.acct.Creds.AccountID, c.acct.ID, true
	}

	ps := s.pool.Public()
	if !ps.Enabled {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "需要本账号的 key"})
		return "", 0, false
	}
	if !ps.Open {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "池子暂不对外开放"})
		return "", 0, false
	}
	if ref == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "匿名调用必须带 account 参数（你自己的上游 account_id）"})
		return "", 0, false
	}
	return ref, s.rowFor(types.AnonPrefix + ref), true
}

// rowFor 把池内 account_id 换成行 ID。没有对应行就回 0（还没为它建过行，
// 也就是这个账号在池子里一条槽位历史都没有）。
func (s *Server) rowFor(accountID string) int64 {
	if accountID == "" {
		return 0
	}
	for _, a := range s.st.Accounts() {
		if a.Creds.AccountID == accountID {
			return a.ID
		}
	}
	return 0
}
