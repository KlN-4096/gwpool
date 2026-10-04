package httpapi

import (
	"net/http"
	"strconv"

	"gwpool/internal/config"
	"gwpool/internal/geography"
	"gwpool/internal/sched"
	"gwpool/internal/types"
)

type mintAllView struct {
	base
	Areas   []mintAllArea
	Keys    []string
	Preview *sched.MintAllStatus
	Notice  string
}

type mintAllArea struct {
	Key, Label string
	Selected   bool
	Countries  int
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
	selectedAreas := map[string]bool{}
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
		selected := map[string]bool{}
		for _, key := range r.PostForm["region"] {
			region, ok := cfg.RegionByKey(key)
			if !ok || !region.Enabled || !geography.Supported(region.Country) {
				http.Error(w, "包含未知或已停用的出口，请重新选择", http.StatusBadRequest)
				return
			}
			selected[key] = true
		}
		for _, key := range r.PostForm["area"] {
			valid := false
			for _, area := range geography.Areas {
				if key == area.Key {
					valid = true
				}
			}
			if !valid {
				http.Error(w, "包含未知大区", http.StatusBadRequest)
				return
			}
			selectedAreas[key] = true
		}
		for _, region := range countryScopeRegions(cfg) {
			if selectedAreas[geography.AreaOf(region.Country).Key] || r.PostForm.Get("all") == "yes" {
				selected[region.Key] = true
			}
		}
		for _, region := range cfg.CandidateRegions(types.Exit{}) {
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
	for _, area := range geography.Areas {
		option := mintAllArea{Key: area.Key, Label: area.Label, Selected: selectedAreas[area.Key]}
		for _, region := range countryScopeRegions(cfg) {
			if geography.AreaOf(region.Country).Key == area.Key {
				option.Countries++
			}
		}
		view.Areas = append(view.Areas, option)
	}
	s.render(w, s.tplMintAll, view)
}

// A selected area expands to one configured exit per supported country, with a
// country-only definition preferred over city aliases. Existing keys stay intact.
func countryScopeRegions(cfg config.Config) []config.ExplorationRegion {
	indices := map[string]int{}
	var out []config.ExplorationRegion
	for _, r := range cfg.RegionDefinitions() {
		if !r.Enabled || !geography.Supported(r.Country) {
			continue
		}
		if index, ok := indices[r.Country]; ok {
			if r.State == "" && r.City == "" {
				out[index] = r
			}
			continue
		}
		indices[r.Country] = len(out)
		out = append(out, r)
	}
	return out
}
