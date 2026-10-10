package httpapi

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"gwpool/internal/config"
	"gwpool/internal/types"
)

// configView 是管理页上「能改的旋钮」那一节。
type configView struct {
	// Models 是候选模型，第一个是当前默认。
	Models []string
	// AllModels 是页面上给勾选的全集：候选清单 ∪ 几个常见的。
	AllModels []modelOpt

	UpstreamPerRegionHour  int
	UpstreamPerAccountHour int
	SlotCooldownMinutes    int
	SpareTarget            int
	MinPairs               int
	RenewPairs             bool
	PublicEnabled          bool
	MinAccounts            int
	PublicLimits           config.PublicWhenSurplus
	// PublicBaseURL 是页面上要告诉人「填进 sub2api 的那个根地址」。
	// 做成配置项而不是从 Host 头推：那个头是调用方可控的输入（见 config 里的注释）。
	PublicBaseURL string

	// Path 显示配置文件在哪，省得人找不到。
	Path string
	// ReadOnly 为真时只显示不给改：没接 Live（或者配置文件写不了）。
	ReadOnly bool
	// Note 是上一次保存的结果提示。
	Note string
	Err  string
}

type modelOpt struct {
	Name     string
	Selected bool
}

// knownModels 是勾选框里的候选来源之一。
//
// **不是白名单**：账号能用哪些模型是上游随时在动的，写死一张表迟早误伤。这里只是让
// 常见的几个不用手打，表单仍然收自定义输入（见 handleAdminConfig 里的 extra）。
var knownModels = []string{
	"gpt-6-astra", "gpt-6-luna", "gpt-5.6-sol", "gpt-5.6", "gpt-5.4",
}

// buildConfigView 拼出配置那一节。live 为 nil 时整节只读。
func buildConfigView(live *config.Live, note, errMsg string) configView {
	v := configView{Note: note, Err: errMsg}
	if live == nil {
		v.ReadOnly = true
		return v
	}
	c := live.Get()
	v.Models = c.Models
	v.UpstreamPerRegionHour = c.UpstreamPerRegionHour
	v.UpstreamPerAccountHour = c.AccountAttemptLimit()
	v.SlotCooldownMinutes = c.SlotCooldownMinutes
	v.SpareTarget = c.SpareTarget
	v.MinPairs = c.MinPairs
	v.RenewPairs = c.RenewPairs
	v.PublicEnabled = c.PublicWhenSurplus.Enabled
	v.MinAccounts = c.PublicWhenSurplus.MinAccounts
	v.PublicLimits = c.PublicWhenSurplus
	v.PublicBaseURL = c.PublicBaseURL
	v.Path = live.Path()

	// 选中的排前面且保持配置里的顺序——第一个是默认模型，顺序有意义。
	seen := map[string]bool{}
	for _, m := range c.Models {
		v.AllModels = append(v.AllModels, modelOpt{Name: m, Selected: true})
		seen[m] = true
	}
	for _, m := range knownModels {
		if !seen[m] {
			v.AllModels = append(v.AllModels, modelOpt{Name: m})
		}
	}
	return v
}

// POST /admin/config 保存旋钮。
//
// 只改表单上出现的那几个键，走 config.Patch——整份 Config 序列化回去会被
// Secret.MarshalJSON 把密钥打码成 "***"，一次保存就毁掉 admin key 和所有 socks 口令。
//
// 保存成功后**不用重启**：Live.Reload 当场把新配置换上去。
func (s *Server) handleAdminConfig(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	live := s.opt.Live
	if live == nil {
		http.Error(w, "这个实例没接配置文件，改不了", http.StatusNotImplemented)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单读不出来", http.StatusBadRequest)
		return
	}

	changes, err := configChanges(r, live.Get())
	if err != nil {
		s.adminConfigDone(w, r, "", err.Error())
		return
	}
	configWriteMu.Lock()
	defer configWriteMu.Unlock()
	if err := config.Patch(live.Path(), changes); err != nil {
		s.opt.Log.Warn("保存配置失败", "err", err)
		s.adminConfigDone(w, r, "", err.Error())
		return
	}
	c, err := live.Reload()
	if err != nil {
		// Patch 已经自己校验过并回滚过了，走到这儿说明是读文件出了别的问题。
		s.adminConfigDone(w, r, "", "存下了但没能热重载："+err.Error())
		return
	}
	s.opt.Log.Info("管理页改了配置", "models", c.Models,
		"upstream_per_region_hour", c.UpstreamPerRegionHour,
		"slot_cooldown", c.SlotCooldown(),
		"public", c.PublicWhenSurplus.Enabled, "min_accounts", c.PublicWhenSurplus.MinAccounts)
	s.adminConfigDone(w, r, "已保存并立即生效", "")
}

// configChanges 把表单翻成要打的补丁。所有数值都在这儿卡边界：它们来自 HTTP 表单，
// 是系统边界。
func configChanges(r *http.Request, cur config.Config) (map[string]any, error) {
	models := r.Form["models"]
	// extra 收手打的模型名，一行一个 / 逗号空格分隔都行。
	models = append(models, strings.FieldsFunc(r.FormValue("models_extra"), func(c rune) bool {
		return c == ',' || c == '\n' || c == '\r' || c == ' ' || c == '\t'
	})...)
	if len(models) == 0 {
		return nil, errEmptyModels
	}
	// 顺序即优先级，第一个是默认模型。合法性交给 config.Patch 里的 Load 去判——
	// 校验规则只该有一份，重复一遍迟早两边不一致。

	perRegionHour, err := atoiField(r, "upstream_per_region_hour", 0, 100000)
	if err != nil {
		return nil, err
	}
	perAccountHour := cur.AccountAttemptLimit()
	if r.Form.Has("upstream_per_account_hour") {
		perAccountHour, err = atoiField(r, "upstream_per_account_hour", 0, 100000)
		if err != nil {
			return nil, err
		}
	}
	// 下限 0 而不是 1：atoiField 对空值回下限，而这一格留空是运营最自然的「让它用默认值」
	// 动作（同一张表单上另外几格的提示就是「0 = 用默认值」，填 0 在原来的下限 1 下反而报错）。
	// 回 1 的话门槛变成「有一个号铸过票就对全互联网开放」——空值失败的方向朝着开那一边。
	// 0 在 config.Patch 里被 Load 当成「没写」⇒ 回去走 AccountsForContinuous 推导。
	minAccounts, err := atoiField(r, "min_accounts", 0, 100000)
	if err != nil {
		return nil, err
	}
	// 历史显示窗口：0使用默认值，上限一周；不再控制交付冷却。
	cooldown, err := atoiField(r, "slot_cooldown_minutes", 0, 7*24*60)
	if err != nil {
		return nil, err
	}
	// Retain legacy bootstrap hints; known-gateway maintenance is independent.
	spare, err := atoiField(r, "spare_target", 0, 100)
	if err != nil {
		return nil, err
	}
	// 上限用 GatewayCeiling 而不是 spare 那个 100：这是绝对张数，而活 pair 一个网关一张
	// ⇒ 比网关总数大就没意义。0 = 不设下界（见 config.Config.MinPairs）。
	minPairs, err := atoiField(r, "min_pairs", 0, types.GatewayCeiling)
	if err != nil {
		return nil, err
	}

	pub := cur.PublicWhenSurplus
	pub.Enabled = truthy(r.FormValue("public_enabled"))
	pub.MinAccounts = minAccounts
	for _, field := range []struct {
		name     string
		dst      *int
		min, max int
	}{
		{"public_requests_per_minute", &pub.RequestsPerMinute, 1, config.MaxPublicRequestsPerMinute},
		{"public_max_concurrent", &pub.MaxConcurrent, 1, config.MaxPublicConcurrent},
		{"public_max_queued", &pub.MaxQueued, 0, config.MaxPublicQueued},
		{"public_queue_wait_ms", &pub.QueueWaitMS, 0, config.MaxPublicQueueWaitMS},
	} {
		// Preserve configured limits when an older form omits these controls.
		if r.Form.Has(field.name) {
			*field.dst, err = atoiField(r, field.name, field.min, field.max)
			if err != nil {
				return nil, err
			}
		}
	}

	return map[string]any{
		"models": models,
		// 形状校验交给 config.Patch 里那遍 Load（normalizeBaseURL）——校验规则只该有
		// 一份，在这儿重复一遍迟早两边不一致。空串是合法的「没配」。
		"public_base_url":           strings.TrimSpace(r.FormValue("public_base_url")),
		"upstream_per_region_hour":  perRegionHour,
		"upstream_per_account_hour": perAccountHour,
		// 未勾续票开关时显式写 false。
		"renew_pairs":           truthy(r.FormValue("renew_pairs")),
		"slot_cooldown_minutes": cooldown,
		"spare_target":          spare,
		"min_pairs":             minPairs,
		"public_when_surplus":   pub,
	}, nil
}

func atoiField(r *http.Request, name string, min, max int) (int, error) {
	raw := strings.TrimSpace(r.FormValue(name))
	if raw == "" {
		return min, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, &fieldErr{name, "要填一个整数"}
	}
	if n < min || n > max {
		return 0, &fieldErr{name, "要在 " + strconv.Itoa(min) + " 和 " + strconv.Itoa(max) + " 之间"}
	}
	return n, nil
}

type fieldErr struct{ field, why string }

func (e *fieldErr) Error() string { return e.field + " " + e.why }

var errEmptyModels = &fieldErr{"models", "至少要选一个"}

// adminConfigDone 保存完回管理页，把结果提示带过去。
//
// 用查询串而不是 session/flash：这个服务没有会话层，而为一句提示引入一个就是本末倒置。
func (s *Server) adminConfigDone(w http.ResponseWriter, r *http.Request, note, errMsg string) {
	q := "?"
	switch {
	case errMsg != "":
		q += "cfg_err=" + url.QueryEscape(errMsg)
	case note != "":
		q += "cfg_note=" + url.QueryEscape(note)
	}
	http.Redirect(w, r, "/admin/"+q+"#config", http.StatusSeeOther)
}
