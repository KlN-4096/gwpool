package httpapi

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"gwpool/internal/types"
)

const keyPageSize = 50
const keyFormMax = 256 << 10
const keyCreationLifetime = 15 * time.Minute

type managedKeyRow struct {
	LastISO                                         string
	LastDate, LastClock, LimitBrief, LimitHint      string
	Key                                             types.AccessKey
	Source, Status, StatusText, Mask, Owner, Limits string
	Created, Last, DetailURL                        string
	Metrics                                         keyMetricView
	Editable, AdminIssued                           bool
}
type keyEndpointRow struct {
	Path  string
	Count uint64
}
type keyHourRow struct {
	Time     string
	Counters types.KeyCounters
}
type keyDetailView struct {
	managedKeyRow
	Endpoints                   []keyEndpointRow
	Hours                       []keyHourRow
	Average, P50, P95, Accounts string
}
type keysView struct {
	ListURL                        string
	SourceLabel, SourceDescription string
	IsAdminSource                  bool
	Tabs                           []keySourceTab
	base
	Rows                                        []managedKeyRow
	Detail                                      *keyDetailView
	Query, Source, Status, Sort, Previous, Next string
	CreationToken, CreatedKeys, Message         string
	Total, Matched, Page, Pages                 int
	Requests, Tickets, RPM                      uint64
	StatsFailed                                 bool
}

type keySourceTab struct {
	Source, Label, URL string
	Count              int
	Selected           bool
}

func keySourceLabel(source string) string {
	switch source {
	case types.AccessKeyUpload:
		return "账号 Key"
	case types.AccessKeyLegacy:
		return "凭证 Key"
	case types.AccessKeyAdmin:
		return "管理员生成 Key"
	default:
		return ""
	}
}

// Keep only list controls in links; a detail selection is never a list filter.
func keyListQuery(values url.Values, source string) url.Values {
	out := url.Values{"source": {source}}
	for _, field := range []string{"q", "status", "sort", "page"} {
		if value := values.Get(field); value != "" {
			out.Set(field, value)
		}
	}
	return out
}

func keyTime(at time.Time) string {
	if at.IsZero() {
		return "—"
	}
	return at.UTC().Format("2006-01-02 15:04:05")
}

func keyLimitText(key types.AccessKey) string {
	if key.Source != types.AccessKeyAdmin {
		return "不限 · 上传者豁免"
	}
	part := func(n int) string {
		if n == 0 {
			return "不限"
		}
		return strconv.Itoa(n)
	}
	return part(key.RPM) + " RPM / " + part(key.Concurrency) + " 并发"
}

func (s *Server) managedKeyRows(store accessKeyStore, now time.Time) ([]managedKeyRow, bool) {
	keys := map[string]types.AccessKey{}
	active := map[string]bool{}
	owners := map[string]string{}
	masks := map[string]string{}
	for _, owner := range s.st.Owners() {
		id := types.AccessKeyID(types.AccessKeyUpload, owner.ID)
		keys[id] = types.AccessKey{ID: id, Source: types.AccessKeyUpload, SubjectID: owner.ID, Label: owner.Label, CreatedAt: owner.CreatedAt}
		_, ready := s.ownerKeyAccount(owner.ID, "")
		secret := s.OwnerKeyFor(owner)
		active[id] = ready && secret != ""
		owners[id] = fmt.Sprintf("上传者 #%d · %s", owner.ID, owner.Label)
		if len(secret) >= 6 {
			masks[id] = "gwp_a_…" + secret[len(secret)-6:]
		}
	}
	for _, account := range s.st.Accounts() {
		if !account.LegacyConsumerKey {
			continue
		}
		id := types.AccessKeyID(types.AccessKeyLegacy, account.ID)
		keys[id] = types.AccessKey{ID: id, Source: types.AccessKeyLegacy, SubjectID: account.ID, Label: account.Label, CreatedAt: account.CreatedAt}
		secret := s.ConsumerKeyFor(account)
		active[id] = account.Enabled && secret != ""
		owners[id] = fmt.Sprintf("凭证 #%d · %s", account.ID, account.Label)
		if len(secret) >= 6 {
			masks[id] = "…" + secret[len(secret)-6:]
		}
	}
	for _, key := range store.AccessKeys() {
		keys[key.ID] = key
		if key.Source == types.AccessKeyAdmin {
			active[key.ID] = true
			owners[key.ID] = "管理员生成 · 独立消费"
			masks[key.ID] = "gwp_k_…" + key.Suffix
		}
	}
	metrics, failed := s.keyMetrics.snapshot(now)
	for id := range metrics {
		if _, exists := keys[id]; !exists {
			source, subject, valid := types.ParseAccessKeyID(id)
			if valid {
				keys[id] = types.AccessKey{ID: id, Source: source, SubjectID: subject, Label: "已移除来源（保留统计）"}
			}
		}
	}
	rows := make([]managedKeyRow, 0, len(keys))
	for id, key := range keys {
		row := managedKeyRow{Key: key, Owner: owners[id], Mask: masks[id], Limits: keyLimitText(key), Metrics: metrics[id],
			Created: keyTime(key.CreatedAt), Last: keyTime(metrics[id].Usage.LastAt),
			DetailURL: "/admin/keys?key=" + url.QueryEscape(id), AdminIssued: key.Source == types.AccessKeyAdmin}
		row.Source = keySourceLabel(key.Source)
		row.LastDate, row.LastClock = "尚无请求", "—"
		if !row.Metrics.Usage.LastAt.IsZero() {
			row.LastISO = row.Metrics.Usage.LastAt.UTC().Format(time.RFC3339)
			row.LastDate = row.Metrics.Usage.LastAt.UTC().Format("2006-01-02")
			row.LastClock = row.Metrics.Usage.LastAt.UTC().Format("15:04:05")
		}
		row.LimitBrief, row.LimitHint = "不限流", "上传者豁免"
		if row.AdminIssued {
			row.LimitBrief, row.LimitHint = "不限", "独立限额"
			if key.RPM > 0 {
				row.LimitBrief = strconv.Itoa(key.RPM) + " RPM"
			}
			if key.Concurrency > 0 {
				row.LimitHint = strconv.Itoa(key.Concurrency) + " 并发"
			}
		}
		switch {
		case !key.RevokedAt.IsZero():
			row.Status, row.StatusText = "revoked", "已撤销"
		case key.Disabled:
			row.Status, row.StatusText = "disabled", "管理停用"
		case !active[id]:
			row.Status, row.StatusText = "inactive", "无有效来源"
		default:
			row.Status, row.StatusText = "active", "可用"
		}
		_, exists := store.AccessKey(id)
		row.Editable = exists && key.RevokedAt.IsZero()
		if row.Mask == "" {
			row.Mask = "—"
		}
		if row.Owner == "" {
			row.Owner = "原上传来源已移除"
		}
		rows = append(rows, row)
	}
	return rows, failed
}

func keyPercentile(c types.KeyCounters, percent uint64) string {
	if c.Requests == 0 {
		return "—"
	}
	target := (c.Requests*percent + 99) / 100
	var seen uint64
	for i, n := range c.Latency {
		seen += n
		if seen >= target {
			if i == len(keyLatencyBounds) {
				return "> 30000 ms"
			}
			return fmt.Sprintf("≤ %d ms", keyLatencyBounds[i])
		}
	}
	return "—"
}

func keyDetail(row managedKeyRow, now time.Time) *keyDetailView {
	d := &keyDetailView{managedKeyRow: row, Average: "—", P50: keyPercentile(row.Metrics.Usage.Totals, 50), P95: keyPercentile(row.Metrics.Usage.Totals, 95)}
	u := row.Metrics.Usage
	if u.Totals.Requests > 0 {
		d.Average = fmt.Sprintf("%.1f ms", float64(u.Totals.DurationMS)/float64(u.Totals.Requests))
	}
	d.Accounts = strconv.Itoa(len(u.AccountHashes))
	if u.AccountsCapped {
		d.Accounts += "+"
	}
	for path, n := range u.Totals.Endpoints {
		d.Endpoints = append(d.Endpoints, keyEndpointRow{path, n})
	}
	slices.SortFunc(d.Endpoints, func(a, b keyEndpointRow) int { return strings.Compare(a.Path, b.Path) })
	cutoff := now.Truncate(time.Hour).Add(-time.Duration(types.KeyUsageHours-1) * time.Hour)
	for _, hour := range u.Hours {
		if !hour.At.Before(cutoff) {
			d.Hours = append(d.Hours, keyHourRow{keyTime(hour.At), hour.Counters})
		}
	}
	slices.SortFunc(d.Hours, func(a, b keyHourRow) int { return strings.Compare(b.Time, a.Time) })
	return d
}

func (s *Server) creationSignature(value string) []byte {
	mac := hmac.New(sha256.New, []byte(s.opt.AdminKey))
	_, _ = mac.Write([]byte("gwpool/key-creation/v1\x00" + value))
	return mac.Sum(nil)
}

func (s *Server) creationToken() (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	value := strconv.FormatInt(time.Now().Add(keyCreationLifetime).Unix(), 10) + "." + hex.EncodeToString(nonce)
	return value + "." + hex.EncodeToString(s.creationSignature(value)), nil
}

func (s *Server) creationRequestID(token string) (string, bool) {
	if len(token) > 128 {
		return "", false
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", false
	}
	expires, err := strconv.ParseInt(parts[0], 10, 64)
	sig, sigErr := hex.DecodeString(parts[2])
	now := time.Now()
	if err != nil || sigErr != nil || expires <= now.Unix() || expires > now.Add(keyCreationLifetime).Unix() ||
		!hmac.Equal(sig, s.creationSignature(parts[0]+"."+parts[1])) {
		return "", false
	}
	// Hash the signed payload, not the hex signature spelling: DecodeString
	// accepts uppercase signatures too, and equivalent tokens are one request.
	hash := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return hex.EncodeToString(hash[:]), true
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	s.renderKeys(w, r, "", "")
}

func (s *Server) renderKeys(w http.ResponseWriter, r *http.Request, created, message string) {
	store, ok := s.st.(accessKeyStore)
	if !ok {
		http.Error(w, "当前存储不支持 Key 管理", http.StatusNotImplemented)
		return
	}
	now := time.Now().UTC()
	rows, failed := s.managedKeyRows(store, now)
	q := r.URL.Query()
	source := q.Get("source")
	if created != "" {
		source = types.AccessKeyAdmin
		q = url.Values{"source": {source}}
	} else if source == "" {
		source = types.AccessKeyUpload
		// Existing direct detail links still open in their own category.
		if detailSource, _, valid := types.ParseAccessKeyID(q.Get("key")); valid {
			source = detailSource
		}
	}
	if keySourceLabel(source) == "" {
		http.Error(w, "Key 分类无效", http.StatusBadRequest)
		return
	}
	token := ""
	if source == types.AccessKeyAdmin {
		var err error
		token, err = s.creationToken()
		if err != nil {
			http.Error(w, "无法生成安全表单", http.StatusInternalServerError)
			return
		}
	}
	listQuery := keyListQuery(q, source)
	view := keysView{base: base{Title: "接入 Key", Mode: s.pool.Mode(), Admin: true, AdminPage: "keys"},
		Query: strings.TrimSpace(q.Get("q")), Source: source, Status: q.Get("status"), Sort: q.Get("sort"),
		CreatedKeys: created, Message: message, CreationToken: token, StatsFailed: failed, Page: 1,
		SourceLabel: keySourceLabel(source), IsAdminSource: source == types.AccessKeyAdmin}
	view.SourceDescription = map[string]string{
		types.AccessKeyUpload: "上传者名下统一使用的接入 Key，不新增独立限流。",
		types.AccessKeyLegacy: "旧版兼容：与单份凭证关联的历史 Key，仅保留原有接入和管理。",
		types.AccessKeyAdmin:  "独立发放的消费 Key，无需上传凭据，可设置每把 Key 的限额。",
	}[source]
	view.ListURL = "/admin/keys?" + listQuery.Encode()
	counts := map[string]int{}
	for _, row := range rows {
		counts[row.Key.Source]++
		if row.Key.Source != source {
			continue
		}
		view.Total++
		detailQuery := keyListQuery(q, source)
		detailQuery.Set("key", row.Key.ID)
		row.DetailURL = "/admin/keys?" + detailQuery.Encode()
		view.Requests += row.Metrics.Usage.Totals.Requests
		view.Tickets += row.Metrics.Usage.Totals.Tickets
		view.RPM += row.Metrics.RPM
		if q.Get("key") == row.Key.ID {
			view.Detail = keyDetail(row, now)
		}
		hay := strings.ToLower(row.Key.ID + " " + row.Key.Label + " " + row.Key.Note + " " + row.Owner + " " + row.Mask)
		if (view.Status == "" || row.Status == view.Status) &&
			strings.Contains(hay, strings.ToLower(view.Query)) {
			view.Rows = append(view.Rows, row)
		}
	}
	for _, category := range []string{types.AccessKeyUpload, types.AccessKeyLegacy, types.AccessKeyAdmin} {
		view.Tabs = append(view.Tabs, keySourceTab{Source: category, Label: keySourceLabel(category),
			URL: "/admin/keys?source=" + category, Count: counts[category], Selected: category == source})
	}
	if q.Get("key") != "" && view.Detail == nil {
		http.Error(w, "当前分类中没有此 Key", http.StatusNotFound)
		return
	}
	slices.SortFunc(view.Rows, func(a, b managedKeyRow) int {
		switch view.Sort {
		case "rpm":
			if a.Metrics.RPM != b.Metrics.RPM {
				if a.Metrics.RPM > b.Metrics.RPM {
					return -1
				}
				return 1
			}
		case "requests":
			if a.Metrics.Usage.Totals.Requests != b.Metrics.Usage.Totals.Requests {
				if a.Metrics.Usage.Totals.Requests > b.Metrics.Usage.Totals.Requests {
					return -1
				}
				return 1
			}
		case "last":
			if !a.Metrics.Usage.LastAt.Equal(b.Metrics.Usage.LastAt) {
				if a.Metrics.Usage.LastAt.After(b.Metrics.Usage.LastAt) {
					return -1
				}
				return 1
			}
		default:
			if !a.Key.CreatedAt.Equal(b.Key.CreatedAt) {
				if a.Key.CreatedAt.After(b.Key.CreatedAt) {
					return -1
				}
				return 1
			}
		}
		return strings.Compare(a.Key.ID, b.Key.ID)
	})
	view.Matched = len(view.Rows)
	view.Pages = max(1, (view.Matched+keyPageSize-1)/keyPageSize)
	if n, err := strconv.Atoi(q.Get("page")); err == nil {
		view.Page = min(max(n, 1), view.Pages)
	}
	link := func(page int) string {
		values := keyListQuery(q, source)
		values.Set("page", strconv.Itoa(page))
		return "/admin/keys?" + values.Encode()
	}
	if view.Page > 1 {
		view.Previous = link(view.Page - 1)
	}
	if view.Page < view.Pages {
		view.Next = link(view.Page + 1)
	}
	start := (view.Page - 1) * keyPageSize
	view.Rows = view.Rows[start:min(start+keyPageSize, view.Matched)]
	s.render(w, s.tplKeys, view)
}

func parseKeyLimit(form url.Values, field string) (int, error) {
	value := strings.TrimSpace(form.Get(field))
	if value == "" {
		return 0, nil
	}
	return strconv.Atoi(value)
}

func (s *Server) handleKeysMutation(w http.ResponseWriter, r *http.Request) {
	if !s.authAdmin(w, r) {
		return
	}
	store, ok := s.st.(accessKeyStore)
	if !ok {
		http.Error(w, "当前存储不支持 Key 管理", http.StatusNotImplemented)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, keyFormMax)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "表单无效或过大", http.StatusBadRequest)
		return
	}
	form := r.PostForm
	action := form.Get("action")
	source := form.Get("source")
	if source != "" && keySourceLabel(source) == "" {
		http.Error(w, "Key 分类无效", http.StatusBadRequest)
		return
	}
	if action == "create" {
		if source != "" && source != types.AccessKeyAdmin {
			http.Error(w, "仅管理员生成分类支持新建 Key", http.StatusBadRequest)
			return
		}
		requestID, valid := s.creationRequestID(form.Get("creation_token"))
		count, e1 := strconv.Atoi(form.Get("count"))
		rpm, e2 := parseKeyLimit(form, "rpm")
		concurrency, e3 := parseKeyLimit(form, "concurrency")
		if !valid || e1 != nil || e2 != nil || e3 != nil {
			http.Error(w, "参数无效或生成表单已过期，请重新打开页面", http.StatusBadRequest)
			return
		}
		keys, err := store.CreateAccessKeysOnce(requestID, count, form.Get("label"), form.Get("note"), rpm, concurrency)
		if err != nil {
			http.Error(w, "未生成：参数不合法、重复提交或存储不可用。请核对当前列表后重试。", http.StatusConflict)
			return
		}
		plain := make([]string, len(keys))
		for i, key := range keys {
			plain[i] = key.Key
		}
		s.renderKeys(w, r, strings.Join(plain, "\n"), "已生成 "+strconv.Itoa(len(keys))+" 把独立 Key")
		return
	}
	if action != "enable" && action != "disable" && action != "revoke" && action != "edit" {
		http.Error(w, "未知操作", http.StatusBadRequest)
		return
	}
	// Source is immutable. Reject mixed or forged category selections before
	// calling the store so a batch can never partially affect another category.
	for _, id := range form["ids"] {
		category, _, valid := types.ParseAccessKeyID(id)
		if !valid || (source != "" && category != source) {
			http.Error(w, "所选 Key 必须属于当前同一分类", http.StatusBadRequest)
			return
		}
		source = category
	}
	if action == "revoke" && form.Get("confirm_revoke") != "yes" {
		http.Error(w, "请确认不可恢复撤销", http.StatusBadRequest)
		return
	}
	patch := types.AccessKeyPatch{Action: action}
	if action == "edit" {
		if len(form["ids"]) != 1 {
			http.Error(w, "编辑必须选择一把 Key", http.StatusBadRequest)
			return
		}
		label, note := form.Get("label"), form.Get("note")
		patch.Label, patch.Note = &label, &note
		for field, dst := range map[string]**int{"rpm": &patch.RPM, "concurrency": &patch.Concurrency} {
			if form.Has(field) {
				value, err := parseKeyLimit(form, field)
				if err != nil {
					http.Error(w, "限额必须为整数", http.StatusBadRequest)
					return
				}
				*dst = &value
			}
		}
	}
	if err := store.UpdateAccessKeys(form["ids"], patch); err != nil {
		http.Error(w, "未变更：选择已失效、已撤销、参数不合法或存储不可用。上传来源不支持独立限额。", http.StatusConflict)
		return
	}
	backQuery := keyListQuery(r.URL.Query(), source)
	if action == "edit" {
		backQuery.Set("key", form.Get("ids"))
	}
	http.Redirect(w, r, "/admin/keys?"+backQuery.Encode(), http.StatusSeeOther)
}
