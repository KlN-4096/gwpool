package httpapi

import "net/http"

func (s *Server) handleAdminMintAll(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	if s.opt.MintAll == nil {
		http.Error(w, "全量打票队列未接入", http.StatusServiceUnavailable)
		return
	}
	s.opt.MintAll.Enqueue()
	http.Redirect(w, r, "/admin/#overview", http.StatusSeeOther)
}
