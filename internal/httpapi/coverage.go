package httpapi

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"gwpool/internal/config"
	"gwpool/internal/coverage"
	"gwpool/internal/proxycatalog"
)

const coverageFormLimit = 64 << 10

type coverageExitOption struct {
	Key, Name string
	Selected  bool
}

type coverageGroupForm struct {
	coverage.Group
	TargetText string
	Exits      []coverageExitOption
}

type coverageView struct {
	base
	Snapshot           coverage.Snapshot
	Regions            []config.ExplorationRegion
	RegionGroups       []displayRegionGroup[config.ExplorationRegion]
	Groups             []coverageGroupForm
	Exits              []coverageExitOption
	Writable           bool
	Note               string
	Error              string
	CliproxyCountries  []proxycatalog.Country
	GlobalCountryCount int
}

func (s *Server) handleCoverage(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	cfg := s.opt.Live.Get()
	now := time.Now()
	view := coverageView{
		base:     base{Title: "区域覆盖与探索出口", Mode: s.pool.Mode()},
		Snapshot: coverage.Build(cfg, s.st.Gateways(), s.effectivePairs(now), now),
		Regions:  cfg.RegionDefinitions(), Writable: s.opt.Live != nil && s.opt.Live.Path() != "",
		Note: r.URL.Query().Get("note"), Error: r.URL.Query().Get("error"),
		CliproxyCountries:  proxycatalog.CliproxyCountries(),
		GlobalCountryCount: len(proxycatalog.GlobalCountries()),
	}
	for _, region := range view.Regions {
		view.Exits = append(view.Exits, coverageExitOption{Key: region.Key, Name: region.Name})
	}
	view.RegionGroups = groupDisplayRegions(cfg, view.Regions, func(region config.ExplorationRegion) string { return region.Key })
	for _, group := range view.Snapshot.Groups {
		form := coverageGroupForm{Group: group, TargetText: strings.Join(group.Targets, "\n")}
		for _, region := range view.Regions {
			selected := false
			for _, key := range group.ExitKeys {
				selected = selected || key == region.Key
			}
			form.Exits = append(form.Exits, coverageExitOption{Key: region.Key, Name: region.Name, Selected: selected})
		}
		view.Groups = append(view.Groups, form)
	}
	s.render(w, s.tplCoverage, view)
}

func (s *Server) handleCoverageConfig(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	if s.opt.Live == nil || s.opt.Live.Path() == "" {
		http.Error(w, "未接入可写配置文件", http.StatusNotImplemented)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, coverageFormLimit)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效或超过 64KiB", http.StatusBadRequest)
		return
	}
	configWriteMu.Lock()
	defer configWriteMu.Unlock()
	cfg := s.opt.Live.Get()
	changes, err := coverageChanges(r, cfg)
	if err == nil {
		err = config.Patch(s.opt.Live.Path(), changes)
	}
	if err == nil {
		_, err = s.opt.Live.Reload()
	}
	query := url.Values{}
	if err != nil {
		query.Set("error", err.Error())
	} else {
		query.Set("note", "已保存并热生效；未立即执行打票")
	}
	http.Redirect(w, r, "/admin/coverage?"+query.Encode(), http.StatusSeeOther)
}

func coverageChanges(r *http.Request, current config.Config) (map[string]any, error) {
	next := current
	key, name := strings.TrimSpace(r.FormValue("key")), strings.TrimSpace(r.FormValue("name"))
	switch r.FormValue("action") {
	case "global-countries":
		expanded, err := config.GlobalCountryRegions(current)
		if err != nil {
			return nil, err
		}
		return map[string]any{"regions": expanded.Regions, "coverage_groups": expanded.CoverageGroups}, nil
	case "region":
		next.Regions = append([]config.ExplorationRegion(nil), current.RegionDefinitions()...)
		region := config.ExplorationRegion{Key: key, Name: name, Country: strings.TrimSpace(r.FormValue("country")),
			State: strings.TrimSpace(r.FormValue("state")), City: strings.TrimSpace(r.FormValue("city")),
			Enabled: r.FormValue("enabled") == "on"}
		existing := false
		for i, old := range next.Regions {
			if old.Key == key {
				next.Regions[i], existing = region, true
				break
			}
		}
		changes := map[string]any{}
		if !existing {
			next.Regions = append(next.Regions, region)
			// 实际出口与分组分开保存；新出口默认附一组未知范围，之后可独立调整。
			next.CoverageGroups = append([]config.CoverageGroup(nil), current.Groups()...)
			groupExists := false
			for _, group := range next.CoverageGroups {
				groupExists = groupExists || group.Key == key
			}
			if !groupExists {
				next.CoverageGroups = append(next.CoverageGroups, config.CoverageGroup{
					Key: key, Name: name, ExitKeys: []string{key},
				})
			}
			changes["coverage_groups"] = next.CoverageGroups
		}
		if err := config.ValidateRegionTransition(current, next); err != nil {
			return nil, err
		}
		changes["regions"] = next.Regions
		return changes, nil
	case "group":
		group := config.CoverageGroup{Key: key, Name: name, ExitKeys: r.Form["exit_keys"],
			Targets: strings.FieldsFunc(r.FormValue("targets"), func(ch rune) bool {
				return ch == ',' || ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
			}), Complete: r.FormValue("complete") == "on"}
		next.CoverageGroups = append([]config.CoverageGroup(nil), current.Groups()...)
		existing := false
		for i, old := range next.CoverageGroups {
			if old.Key == key {
				next.CoverageGroups[i], existing = group, true
				break
			}
		}
		if !existing {
			next.CoverageGroups = append(next.CoverageGroups, group)
		}
		if err := config.ValidateRegionTransition(current, next); err != nil {
			return nil, err
		}
		return map[string]any{"coverage_groups": next.CoverageGroups}, nil
	default:
		return nil, fmt.Errorf("未知的覆盖配置操作")
	}
}
