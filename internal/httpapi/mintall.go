package httpapi

import (
	"net/http"
	"strconv"

	"gwpool/internal/config"
	"gwpool/internal/sched"
)

type mintAllView struct {
	base
	Groups  []displayRegionGroup[coverageExitOption]
	Keys    []string
	Preview *sched.MintAllStatus
	Notice  string
}

func (s *Server) handleAdminMintAll(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	if s.opt.MintAll == nil {
		http.Error(w, "全量打票队列未接入", http.StatusServiceUnavailable)
		return
	}
	cfg := s.opt.Live.Get()
	view := mintAllView{base: base{Title: "按范围打票", Mode: s.pool.Mode()}}
	selected := map[string]bool{}
	for _, region := range config.DefaultRegions() {
		selected[region.Key] = true
	}
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(w, r.Body, coverageFormLimit)
		if err := r.ParseForm(); err != nil {
			http.Error(w, "表单无效或超过64KiB", http.StatusBadRequest)
			return
		}
		action := r.PostForm.Get("action")
		if action != "preview" && action != "start" {
			http.Error(w, "请先选择范围并预览", http.StatusBadRequest)
			return
		}
		selected = map[string]bool{}
		for _, key := range r.PostForm["region"] {
			region, ok := cfg.RegionByKey(key)
			if !ok || !region.Enabled {
				http.Error(w, "包含未知或已停用的出口，请重新选择", http.StatusBadRequest)
				return
			}
			selected[key] = true
		}
		for _, region := range cfg.RegionDefinitions() {
			for _, area := range r.PostForm["area"] {
				if region.Enabled && config.CountryArea(region.Country) == area {
					selected[region.Key] = true
				}
			}
			if region.Enabled && r.PostForm.Get("all") == "yes" {
				selected[region.Key] = true
			}
			if selected[region.Key] {
				view.Keys = append(view.Keys, region.Key)
			}
		}
		if len(view.Keys) == 0 {
			http.Error(w, "至少选择一个启用出口", http.StatusBadRequest)
			return
		}
		preview := s.opt.MintAll.Preview(view.Keys...)
		view.Preview = &preview
		if action == "start" {
			total, errTotal := strconv.Atoi(r.PostForm.Get("total"))
			queued, errQueued := strconv.Atoi(r.PostForm.Get("queued"))
			if errTotal != nil || errQueued != nil {
				http.Error(w, "缺少预览确认", http.StatusBadRequest)
				return
			}
			confirmed, accepted := s.opt.MintAll.Confirm(total, queued, view.Keys...)
			if accepted {
				http.Redirect(w, r, "/admin/#overview", http.StatusSeeOther)
				return
			}
			view.Preview = &confirmed
			view.Notice = "库存、账号或额度已变化，请核对更新后的预览再确认。"
		}
	}
	var options []coverageExitOption
	for _, region := range cfg.RegionDefinitions() {
		if region.Enabled {
			options = append(options, coverageExitOption{Key: region.Key, Name: region.Name, Selected: selected[region.Key]})
		}
	}
	view.Groups = groupDisplayRegions(cfg, options, func(option coverageExitOption) string { return option.Key })
	s.render(w, s.tplMintAll, view)
}
