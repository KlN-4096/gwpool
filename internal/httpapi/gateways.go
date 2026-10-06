package httpapi

import (
	"net/http"
	"sort"
	"strings"
	"time"

	"gwpool/internal/coverage"
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
	r = s.resolveCallerOnce(r)
	// 占位行在这条路上照常用：used_by_you 问的就是「那个上游账号」的槽位历史。
	upstream, acctID, _, ok := s.whoAsks(w, r)
	if !ok {
		return
	}

	now := time.Now()
	recommendations := s.cooldownRecommendations(r, now)
	contactStats := s.encodedContactStats(r, now)
	model := strings.TrimSpace(r.URL.Query().Get("model"))
	if model != "" && !validContactToken(model) {
		s.badRequest(w, http.StatusBadRequest, "model 格式错误")
		return
	}
	priorities := s.contactPriorities(model, now)
	pairs := s.cachedCatalogPairs(now)
	// 按网关名收一份这个账号自己的槽位：槽位表的键格式归 store 所有，
	// 这里只认 Slot 自己带的两个字段，免得为一个拼键函数把 store 拖进来。
	mine := map[string]types.Slot{}
	if acctID != 0 {
		for _, sl := range s.slotsForAccount(acctID) {
			if sl.AccountID == acctID {
				mine[sl.Gateway] = sl
			}
		}
	}

	// These two durations remain protocol constants, not cached configuration.
	// Read readiness, caller history and runtime target anew on every request.
	out := gatewayCatalogResponse{gatewayCatalogHeader: gatewayCatalogHeader{
		Account: upstream, Target: s.pool.PairTarget(),
		DeliverTTLS:   int(types.DeliverTTL / time.Second),
		DeliverFloorS: int(types.DeliverFloor / time.Second)}}

	cooldown := s.slotCooldown()
	for name := range s.st.Gateways() {
		p, hasPair := pairs[name]
		left := p.ExpiresAt.Sub(now)
		// **只列此刻真能拿到票的网关**：客户端是照这张清单点名的，而点名一个没有活
		// pair 的网关只会换回一个 503（现铸要先摸到它，那是发现路径的事，不是点名）。
		// 把「池子知道的所有网关」全列出来等于让它一个个去试。
		if !hasPair || left < types.DeliverFloor {
			continue
		}
		out.Live++
		it := gatewayCatalogRow{Name: name, PairReady: true, ValidForS: int(left.Seconds()), Contacts: contactStats[name]}
		it.DatacenterCountry = coverage.DatacenterCountry(name)
		if priority, ok := priorities[name]; ok {
			it.Priority = &priority
		}
		if recommendation, ok := recommendations[name]; ok {
			it.Cooldown = &recommendation
		}
		if sl, ok := mine[name]; ok && !sl.LastTouch.IsZero() {
			// 按 LastTouch 算，**不按 LastVerdict**：判定只有上传者那条交付路径写得出来，
			// 跨账号和匿名的交付一个判定都不落 ⇒ 按判定算的话外部消费者看到的永远是
			// 「全都没用过」，照这张清单挑等于没挑。LastTouch 两条路都写，扫描和铸票
			// 也写——那些同样烧了这个账号在这个网关上的窗口。
			//
			// 还要卡冷却：烧过要出冷却才重新给满血，三天前那一次与现在无关。
			// 不卡的话用久了每个网关都是「用过」，客户端永远挑不出落点。
			// 读实时配置，别读常量——那个数是运营按当天观测调的（slot_cooldown_minutes）。
			it.UsedByYou = now.Sub(sl.LastTouch) < cooldown
			it.LastUsedAt = sl.LastTouch.UTC().Format(time.RFC3339)
		}
		out.Gateways = append(out.Gateways, it)
	}
	// 稳定顺序：客户端按这个列表挑，每次换序会让它的选择无端抖动。
	sort.Slice(out.Gateways, func(i, j int) bool { return out.Gateways[i].Name < out.Gateways[j].Name })

	if err := writeGatewayCatalog(w, out); err != nil {
		s.opt.Log.Error("encode gateway catalog failed", "error", err)
		http.Error(w, "清单编码失败", http.StatusInternalServerError)
	}
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
// 第三个返回值说「解出来的是占位行吗」（`anon:` 命名空间，没有任何归属证据）。
// 两个端点对它的态度刻意相反：取票照占位行算（方向保守，见 types.AnonPrefix），
// 归还一律拒（方向危险）。调用方自己按前缀判就是第二把尺子，所以由这儿一次说清。
func (s *Server) whoAsks(w http.ResponseWriter, r *http.Request) (upstream string, row int64, placeholder, ok bool) {
	ref := strings.TrimSpace(r.URL.Query().Get("account"))
	if len(ref) > 128 {
		s.badRequest(w, http.StatusBadRequest, "account 太长")
		return "", 0, false, false
	}

	if c, ok := s.resolveCaller(r); ok {
		if c.public {
			if ref == "" {
				s.badRequest(w, http.StatusBadRequest, "公共接入必须带 account 参数")
				return "", 0, false, false
			}
			return ref, s.rowFor(types.AnonPrefix + ref), true, true
		}
		if c.admin {
			// 管理员没有自己的账号：槽位是 (账号 × 网关)，猜一个就等于把 A 的历史报成 B 的。
			if ref == "" {
				s.badRequestFor(w, http.StatusBadRequest, 0, "管理员调用必须带 account 参数指明替谁")
				return "", 0, false, false
			}
			// 和 /cookie 一样按托管行解析：管理员那条路取的是那一行自己的槽位。
			a, found := s.findAccount(ref)
			if !found {
				s.badRequestFor(w, http.StatusNotFound, 0, "没有这个账号")
				return "", 0, false, false
			}
			return ref, a.ID, false, true
		}
		if ref != "" && !accountRefMatches(c.acct, ref) {
			// 跨账号：/cookie 把这种票算在占位行上，这边也只能读那一行。
			return ref, s.rowFor(types.AnonPrefix + ref), true, true
		}
		return c.acct.Creds.AccountID, c.acct.ID, false, true
	}

	s.unauthorized(w, "需要有效的接入 Key")
	return "", 0, false, false
}

// slotCooldown 是当前生效的槽位冷却时长。没接配置文件（测试）就用默认值。
func (s *Server) slotCooldown() time.Duration {
	if s.opt.Live == nil {
		return types.SlotCooldown
	}
	return s.opt.Live.Get().SlotCooldown()
}

// rowFor 把池内 account_id 换成行 ID。没有对应行就回 0（还没为它建过行，
// 也就是这个账号在池子里一条槽位历史都没有）。
func (s *Server) rowFor(accountID string) int64 {
	if accountID == "" {
		return 0
	}
	if indexed, ok := s.st.(interface {
		AccountByUpstreamID(string) (types.Account, bool)
	}); ok {
		account, _ := indexed.AccountByUpstreamID(accountID)
		return account.ID
	}
	for _, a := range s.st.Accounts() {
		if a.Creds.AccountID == accountID {
			return a.ID
		}
	}
	return 0
}
