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
// All keys use an explicit external member reference. Stored consumer touches
// are historical telemetry only and never personalize this live directory.
func (s *Server) handleGateways(w http.ResponseWriter, r *http.Request) {
	r = s.resolveCallerOnce(r)
	_, ok := s.resolveCaller(r)
	if !ok {
		s.unauthorized(w, "需要有效的接入 Key")
		return
	}
	upstream := strings.TrimSpace(r.URL.Query().Get("account"))
	if !validConsumerRef(upstream) {
		s.badRequest(w, http.StatusBadRequest, "目录查询必须带 account 参数（完整的上游成员标识）；请升级到最新版 Sub2api")
		return
	}
	s.writeCatalogFor(w, r, upstream)
}

func (s *Server) writeCatalogFor(w http.ResponseWriter, r *http.Request, upstream string) {
	now := time.Now()
	model := strings.TrimSpace(r.URL.Query().Get("model"))
	if model != "" && !validContactToken(model) {
		s.badRequest(w, http.StatusBadRequest, "model 格式错误")
		return
	}
	recommendations := s.cooldownRecommendations(r, now)
	contactStats, priorities := s.catalogContacts(r, model, now)
	pairs := s.cachedCatalogPairs(now)
	out := gatewayCatalogResponse{gatewayCatalogHeader: gatewayCatalogHeader{
		Account: upstream, Target: s.pool.PairTarget(),
		SnapshotAt:    now.UTC(),
		DeliverFloorS: int(types.DeliverFloor / time.Second)}}

	for _, name := range s.gatewayNames() {
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
		out.Gateways = append(out.Gateways, it)
	}
	// 稳定顺序：客户端按这个列表挑，每次换序会让它的选择无端抖动。
	sort.Slice(out.Gateways, func(i, j int) bool { return out.Gateways[i].Name < out.Gateways[j].Name })

	if err := writeGatewayCatalog(w, out); err != nil {
		s.opt.Log.Error("encode gateway catalog failed", "error", err)
		http.Error(w, "清单编码失败", http.StatusInternalServerError)
	}
}

func (s *Server) gatewayNames() []string {
	if narrow, ok := s.st.(interface{ GatewayNames() []string }); ok {
		return narrow.GatewayNames()
	}
	all := s.st.Gateways()
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	return names
}

func (s *Server) hasGateway(name string) bool {
	if narrow, ok := s.st.(interface{ HasGateway(string) bool }); ok {
		return narrow.HasGateway(name)
	}
	_, ok := s.st.Gateways()[name]
	return ok
}

// slotCooldown 是当前生效的槽位冷却时长。没接配置文件（测试）就用默认值。
func (s *Server) slotCooldown() time.Duration {
	if s.opt.Live == nil {
		return types.SlotCooldown
	}
	return s.opt.Live.Get().SlotCooldown()
}
