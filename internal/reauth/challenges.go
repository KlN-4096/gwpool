package reauth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"strings"
	"time"
)

func objects(v any, key string, depth int) []map[string]any {
	if depth > 8 {
		return nil
	}
	switch value := v.(type) {
	case map[string]any:
		if list, ok := value[key].([]any); ok {
			var out []map[string]any
			for _, item := range list {
				if m, ok := item.(map[string]any); ok {
					out = append(out, m)
				}
			}
			return out
		}
		for _, nested := range value {
			if found := objects(nested, key, depth+1); len(found) > 0 {
				return found
			}
		}
	case []any:
		for _, nested := range value {
			if found := objects(nested, key, depth+1); len(found) > 0 {
				return found
			}
		}
	}
	return nil
}

func (f *flow) session(ctx context.Context) (map[string]any, error) {
	origin, _ := url.Parse(authOrigin)
	for _, cookie := range f.c.http.Jar.Cookies(origin) {
		if cookie.Name != "oai-client-auth-session" || len(cookie.Value) > 64<<10 {
			continue
		}
		value, err := url.QueryUnescape(cookie.Value)
		if err != nil {
			continue
		}
		value = strings.TrimPrefix(value, "s:")
		candidates := []string{value}
		for _, part := range strings.Split(value, ".") {
			if raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(part, "=")); err == nil {
				candidates = append(candidates, string(raw))
			}
		}
		for _, candidate := range candidates {
			var session map[string]any
			if json.Unmarshal([]byte(candidate), &session) == nil && session != nil {
				if nested, ok := session["client_auth_session"].(map[string]any); ok {
					session = nested
				}
				return session, nil
			}
		}
	}
	p, err := f.request(ctx, "GET", authOrigin+"/api/accounts/client_auth_session_dump", nil)
	if err != nil {
		return nil, err
	}
	if nested, ok := p.data["client_auth_session"].(map[string]any); ok {
		return nested, nil
	}
	return p.data, nil
}

func factorID(data map[string]any) string {
	for _, key := range []string{"mfa_factors", "factors"} {
		for _, factor := range objects(data, key, 0) {
			kind := field(factor, "factor_type")
			if kind == "" {
				kind = field(factor, "type")
			}
			if kind == "totp" && field(factor, "id") != "" {
				return field(factor, "id")
			}
		}
	}
	return ""
}

func (f *flow) mfa(ctx context.Context, p page) (page, error) {
	if f.didMFA {
		return page{}, &Error{Code: "totp_rejected"}
	}
	f.didMFA = true
	id := factorID(p.data)
	if id == "" {
		session, err := f.session(ctx)
		if err != nil {
			return page{}, err
		}
		id = factorID(session)
	}
	if id == "" {
		return page{}, &Error{Code: "unsupported_mfa"}
	}
	f.progress("totp")
	if _, err := f.request(ctx, "POST", authOrigin+"/api/accounts/mfa/issue_challenge",
		map[string]any{"id": id, "type": "totp", "force_fresh_challenge": false}); err != nil {
		return page{}, err
	}
	var code string
	var err error
	if f.secret != "" {
		code, err = totp(f.secret, time.Now())
		f.secret = ""
	} else {
		code, err = f.prompt(ctx, Challenge{Kind: "totp_code"})
	}
	if err != nil {
		return page{}, err
	}
	if !validCode(code) {
		return page{}, &Error{Code: "invalid_code"}
	}
	return f.request(ctx, "POST", authOrigin+"/api/accounts/mfa/verify",
		map[string]string{"id": id, "type": "totp", "code": code})
}

func choices(data map[string]any, keys ...string) []Choice {
	for _, key := range keys {
		var out []Choice
		for _, item := range objects(data, key, 0) {
			id := field(item, "id")
			if id == "" {
				continue
			}
			label := field(item, "name")
			if label == "" {
				label = field(item, "title")
			}
			if label == "" {
				label = "工作区 " + string(rune('A'+len(out)))
			}
			if len(label) > 128 {
				label = label[:128]
			}
			out = append(out, Choice{ID: id, Label: label})
			if len(out) == 32 {
				break
			}
		}
		if len(out) > 0 {
			return out
		}
	}
	return nil
}

func (f *flow) choose(ctx context.Context, kind string, items []Choice) (string, error) {
	if len(items) == 0 {
		return "", &Error{Code: "workspace_unavailable"}
	}
	if len(items) == 1 {
		return items[0].ID, nil
	}
	id, err := f.prompt(ctx, Challenge{Kind: kind, Choices: items})
	if err != nil {
		return "", err
	}
	for _, item := range items {
		if item.ID == id {
			return id, nil
		}
	}
	return "", &Error{Code: "invalid_selection"}
}

func (f *flow) selectWorkspace(ctx context.Context) (page, error) {
	f.progress("authorize")
	session, err := f.session(ctx)
	if err != nil {
		return page{}, err
	}
	id := field(session, "workspace_id")
	if id == "" {
		id, err = f.choose(ctx, "workspace", choices(session, "workspaces"))
		if err != nil {
			return page{}, err
		}
	}
	p, err := f.request(ctx, "POST", authOrigin+"/api/accounts/workspace/select", map[string]string{"workspace_id": id})
	if err != nil {
		return p, err
	}
	if p.is("organization_select") || len(choices(p.data, "orgs", "organizations")) > 0 {
		return f.selectOrganization(ctx, p)
	}
	return p, nil
}

func (f *flow) selectOrganization(ctx context.Context, p page) (page, error) {
	f.progress("authorize")
	orgs := choices(p.data, "orgs", "organizations")
	if len(orgs) == 0 {
		return page{}, &Error{Code: "workspace_unavailable"}
	}
	id, err := f.choose(ctx, "organization", orgs)
	if err != nil {
		return page{}, err
	}
	body := map[string]string{"org_id": id}
	// Never silently choose an arbitrary project when multiple choices exist.
	for _, key := range []string{"orgs", "organizations"} {
		for _, org := range objects(p.data, key, 0) {
			if field(org, "id") != id {
				continue
			}
			if projects := choices(org, "projects"); len(projects) > 0 {
				project, err := f.choose(ctx, "project", projects)
				if err != nil {
					return page{}, err
				}
				body["project_id"] = project
			}
		}
	}
	return f.request(ctx, "POST", authOrigin+"/api/accounts/organization/select", body)
}
