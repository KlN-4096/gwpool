package httpapi

import (
	"context"
	"net/http"
	"time"

	"gwpool/internal/types"
)

type accessKeyStore interface {
	AccessKeys() []types.AccessKey
	AccessKey(string) (types.AccessKey, bool)
	ResolveIssuedAccessKey(string) (types.AccessKey, bool)
	CreateAccessKeys(int, string, string, int, int) ([]types.CreatedAccessKey, error)
	CreateAccessKeysOnce(string, int, string, string, int, int) ([]types.CreatedAccessKey, error)
	CreateAdminKeysOnce(string, int, string, string) ([]types.CreatedAccessKey, error)
	UpdateAccessKeys([]string, types.AccessKeyPatch) error
	AccessKeyUsage() map[string]types.KeyUsage
	SaveAccessKeyUsage(map[string]types.KeyUsage) error
}

func (s *Server) accessKeyPolicy(source string, id int64) types.AccessKey {
	keyID := types.AccessKeyID(source, id)
	if store, ok := s.st.(accessKeyStore); ok {
		if key, found := store.AccessKey(keyID); found {
			return key
		}
	}
	return types.AccessKey{ID: keyID, Source: source, SubjectID: id}
}

type keyTicketCounterContext struct{}

type keyResponse struct {
	http.ResponseWriter
	status, bytes int
	writeFailed   bool
}

func (w *keyResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *keyResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *keyResponse) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	if err != nil || n != len(data) {
		w.writeFailed = true
	}
	return n, err
}

func (s *Server) serveManagedKey(w http.ResponseWriter, r *http.Request, c caller, authorized bool, next http.HandlerFunc) {
	started := time.Now()
	admitted, retry := s.keyMetrics.begin(c.key, authorized, started)
	recorder := &keyResponse{ResponseWriter: w}
	tickets := 0
	defer func() {
		status := recorder.status
		if status == 0 {
			status = http.StatusInternalServerError
		}
		if recorder.writeFailed || status < 200 || status >= 300 {
			tickets = 0
		}
		s.keyMetrics.finish(c.key.ID, r, started, status, recorder.bytes, tickets, admitted)
	}()
	if !authorized {
		s.unauthorized(recorder, "接入 Key 已停用、撤销或没有有效上传凭据")
		return
	}
	if !admitted {
		writeFail(recorder, http.StatusServiceUnavailable, apiError{
			Code: codeRateLimited, Message: "此接入 Key 达到独立请求上限，请稍后重试",
			RetryAfterSeconds: retrySeconds(retry),
		})
		return
	}
	r = r.WithContext(context.WithValue(r.Context(), keyTicketCounterContext{}, &tickets))
	next(recorder, r)
}
