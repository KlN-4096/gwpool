package httpapi

import (
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"gwpool/internal/config"
)

// configView 是管理页上「能改的旋钮」那一节。
type configView struct {
	// Models 是候选模型，第一个是当前默认。
	Models []string
	// AllModels 是页面上给勾选的全集：候选清单 ∪ 几个常见的。
	AllModels []modelOpt

	UpstreamPerHour int
	PublicEnabled   bool
	MinFreeGateways int

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
	v.UpstreamPerHour = c.UpstreamPerHour
	v.PublicEnabled = c.PublicWhenSurplus.Enabled
	v.MinFreeGateways = c.PublicWhenSurplus.MinFreeGateways
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
		"upstream_per_hour", c.UpstreamPerHour,
		"public", c.PublicWhenSurplus.Enabled, "min_free_gateways", c.PublicWhenSurplus.MinFreeGateways)
	s.adminConfigDone(w, r, "已保存并立即生效", "")
}

// configChanges 把表单翻成要打的补丁。所有数值都在这儿卡边界：它们来自 HTTP 表单，
// 是系统边界。
func configChanges(r *http.Request, cur config.Config) (map[string]any, error) {
	models := r.Form["models"]
	// extra 收手打的模型名，一行一个 / 逗号空格分隔都行。
	for _, f := range strings.FieldsFunc(r.FormValue("models_extra"), func(c rune) bool {
		return c == ',' || c == '\n' || c == '\r' || c == ' ' || c == '\t'
	}) {
		models = append(models, f)
	}
	if len(models) == 0 {
		return nil, errEmptyModels
	}
	// 顺序即优先级，第一个是默认模型。合法性交给 config.Patch 里的 Load 去判——
	// 校验规则只该有一份，重复一遍迟早两边不一致。

	perHour, err := atoiField(r, "upstream_per_hour", 0, 100000)
	if err != nil {
		return nil, err
	}
	minFree, err := atoiField(r, "min_free_gateways", 1, 100000)
	if err != nil {
		return nil, err
	}

	pub := cur.PublicWhenSurplus
	pub.Enabled = truthy(r.FormValue("public_enabled"))
	pub.MinFreeGateways = minFree

	return map[string]any{
		"models":              models,
		"upstream_per_hour":   perHour,
		"public_when_surplus": pub,
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
