package httpapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/types"
)

const cleanupPreviewLifetime = 10 * time.Minute
const cleanupFormLimit = 128 << 10
const cleanupCandidateLimit = 500

type cleanupStore interface {
	DeleteUnauthorizedAccounts([]types.AccountCleanupCandidate) (int, error)
}

type cleanupManifest struct {
	Expires    int64                           `json:"expires"`
	Candidates []types.AccountCleanupCandidate `json:"candidates"`
}

type cleanupRow struct {
	ID          int64
	Label, Plan string
}

type cleanupView struct {
	base
	Rows             []cleanupRow
	Token, Expires   string
	Finished         bool
	Deleted, Skipped int
}

func (s *Server) cleanupSignature(payload string) []byte {
	mac := hmac.New(sha256.New, []byte(s.opt.AdminKey))
	_, _ = mac.Write([]byte("gwpool/cleanup-401/v1\x00" + payload))
	return mac.Sum(nil)
}

func (s *Server) handleCleanupPreview(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	view := cleanupView{base: base{Title: "清理 401 凭据", Mode: s.pool.Mode(), Admin: true, AdminPage: "accounts"}}
	manifest := cleanupManifest{Expires: time.Now().Add(cleanupPreviewLifetime).Unix()}
	for _, a := range s.st.Accounts() {
		if types.UnauthorizedCredential(a) {
			view.Rows = append(view.Rows, cleanupRow{ID: a.ID, Label: a.Label, Plan: a.PlanType})
			manifest.Candidates = append(manifest.Candidates, types.CleanupCandidate(a))
		}
	}
	data, err := json.Marshal(manifest)
	if err != nil || len(manifest.Candidates) > cleanupCandidateLimit {
		http.Error(w, "无法生成清理预览", http.StatusInternalServerError)
		return
	}
	payload := base64.RawURLEncoding.EncodeToString(data)
	view.Token = payload + "." + base64.RawURLEncoding.EncodeToString(s.cleanupSignature(payload))
	view.Expires = time.Unix(manifest.Expires, 0).UTC().Format(time.DateTime)
	s.render(w, s.tplCleanup, view)
}

func (s *Server) handleCleanupConfirm(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, cleanupFormLimit)
	if err := r.ParseForm(); err != nil || r.PostForm.Get("confirm") != "yes" {
		http.Error(w, "请先预览并确认清理范围", http.StatusBadRequest)
		return
	}
	payload, signature, ok := strings.Cut(r.PostForm.Get("preview"), ".")
	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if !ok || err != nil || !hmac.Equal(sig, s.cleanupSignature(payload)) {
		http.Error(w, "清理预览无效，请重新预览", http.StatusBadRequest)
		return
	}
	data, err := base64.RawURLEncoding.DecodeString(payload)
	var manifest cleanupManifest
	now := time.Now()
	if err != nil || json.Unmarshal(data, &manifest) != nil ||
		manifest.Expires <= now.Unix() || manifest.Expires > now.Add(cleanupPreviewLifetime).Unix() ||
		len(manifest.Candidates) == 0 || len(manifest.Candidates) > cleanupCandidateLimit {
		http.Error(w, "清理预览已失效，请重新预览", http.StatusBadRequest)
		return
	}
	store, ok := s.st.(cleanupStore)
	if !ok {
		http.Error(w, "当前存储不支持批量清理", http.StatusNotImplemented)
		return
	}
	deleted, err := store.DeleteUnauthorizedAccounts(manifest.Candidates)
	if err != nil {
		s.opt.Log.Error("401 凭据清理失败", "err", err)
		http.Error(w, "清理未完成，未确认删除成功，请重新检查状态", http.StatusInternalServerError)
		return
	}
	// PRG: refresh must not repeat the destructive POST. Counts carry no secrets.
	http.Redirect(w, r, "/admin/accounts/cleanup-result?deleted="+strconv.Itoa(deleted)+
		"&skipped="+strconv.Itoa(len(manifest.Candidates)-deleted), http.StatusSeeOther)
}

func (s *Server) handleCleanupResult(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	view := cleanupView{base: base{Title: "清理结果", Mode: s.pool.Mode(), Admin: true, AdminPage: "accounts"}, Finished: true}
	for key, dst := range map[string]*int{"deleted": &view.Deleted, "skipped": &view.Skipped} {
		n, err := strconv.Atoi(r.URL.Query().Get(key))
		if err != nil || n < 0 || n > cleanupCandidateLimit {
			http.Error(w, "结果参数无效", http.StatusBadRequest)
			return
		}
		*dst = n
	}
	s.render(w, s.tplCleanup, view)
}
