package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

const reportBatchMax = 32
const reportBatchBodyMax = 64 * 1024
const reportBatchResponseMax = 64 * 1024

// Covers status/added, a maximal cooldown recommendation and JSON punctuation.
// IDs are separately measured using the same encoder escaping as the response.
const reportBatchResultReserve = 1024
const reportBatchEnvelopeReserve = 64

type rawReportItem struct {
	id  string
	raw json.RawMessage
}

// Parse the envelope before making any writes. Individual schemas are decoded
// later, so a bad field in one report cannot discard its valid neighbours.
func (s *Server) readReportBatch(w http.ResponseWriter, r *http.Request) ([]rawReportItem, bool) {
	var envelope struct {
		Reports []json.RawMessage `json:"reports"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, reportBatchBodyMax))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		s.badRequest(w, http.StatusBadRequest, "批量样本格式错误")
		return nil, false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF ||
		len(envelope.Reports) == 0 || len(envelope.Reports) > reportBatchMax {
		s.badRequest(w, http.StatusBadRequest, "批量样本应为1至32条")
		return nil, false
	}
	items := make([]rawReportItem, 0, len(envelope.Reports))
	seen := make(map[string]bool, len(envelope.Reports))
	responseBudget := reportBatchEnvelopeReserve
	for _, raw := range envelope.Reports {
		var identity struct {
			ID *string `json:"id"`
		}
		// Invalid/missing IDs are item validation errors, not envelope errors.
		decodedID := json.Unmarshal(raw, &identity) == nil && identity.ID != nil
		originalID := ""
		if decodedID {
			originalID = *identity.ID
		}
		id := strings.ToLower(originalID)
		if decodedID && seen[id] {
			s.badRequest(w, http.StatusBadRequest, "批量样本 ID 不得重复")
			return nil, false
		}
		if decodedID {
			seen[id] = true
		}
		encodedID, _ := json.Marshal(originalID) // strings always encode successfully
		responseBudget += len(encodedID) + reportBatchResultReserve
		if responseBudget > reportBatchResponseMax {
			s.badRequest(w, http.StatusBadRequest, "批量样本 ID 超出响应容量")
			return nil, false
		}
		items = append(items, rawReportItem{id: originalID, raw: raw})
	}
	return items, true
}

func decodeReportItem(raw json.RawMessage, maxBytes int, dst any) bool {
	if len(raw) > maxBytes || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(dst) == nil
}
