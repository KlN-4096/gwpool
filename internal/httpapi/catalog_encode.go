package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"

	"gwpool/internal/types"
)

type gatewayCatalogRow struct {
	Name              string                        `json:"name"`
	PairReady         bool                          `json:"pair_ready"`
	ValidForS         int                           `json:"valid_for_s"`
	UsedByYou         bool                          `json:"used_by_you"`
	LastUsedAt        string                        `json:"last_used_at,omitempty"`
	Cooldown          *types.CooldownRecommendation `json:"cooldown,omitempty"`
	Contacts          json.RawMessage               `json:"contacts,omitempty"`
	Priority          *types.GatewayPriority        `json:"priority,omitempty"`
	DatacenterCountry string                        `json:"datacenter_country,omitempty"`
}

type gatewayCatalogHeader struct {
	Account       string `json:"account"`
	Live          int    `json:"live"`
	Target        int    `json:"target"`
	DeliverTTLS   int    `json:"deliver_ttl_s"`
	DeliverFloorS int    `json:"deliver_floor_s"`
}

type gatewayCatalogResponse struct {
	gatewayCatalogHeader
	Gateways []gatewayCatalogRow `json:"gateways"`
}

// Contacts is produced only by json.Marshal inside encodedContactStats/
// encodeContacts, never by a request body or unchecked raw input. Copy those
// immutable encoded arrays directly instead of having encoding/json parse and
// compact the same historical statistics on every catalog request.
func encodeGatewayCatalog(out *bytes.Buffer, response gatewayCatalogResponse) error {
	const estimatedDynamicRowBytes = 256
	capacity := len(response.Gateways) * estimatedDynamicRowBytes
	for _, row := range response.Gateways {
		capacity += len(row.Contacts)
	}
	out.Grow(capacity)
	encoder := json.NewEncoder(out)
	if err := encoder.Encode(response.gatewayCatalogHeader); err != nil {
		return err
	}
	out.Truncate(out.Len() - 2) // closing brace plus Encoder's newline
	out.WriteString(`,"gateways":`)
	if response.Gateways == nil {
		out.WriteString("null}\n")
		return nil
	}
	out.WriteByte('[')
	for i, row := range response.Gateways {
		if i > 0 {
			out.WriteByte(',')
		}
		contacts := row.Contacts
		row.Contacts = nil
		if err := encoder.Encode(row); err != nil {
			return err
		}
		out.Truncate(out.Len() - 1) // strip only Encoder's newline
		if len(contacts) == 0 {
			continue
		}
		out.Truncate(out.Len() - 1)
		out.WriteString(`,"contacts":`)
		out.Write(contacts)
		out.WriteByte('}')
	}
	out.WriteString("]}\n")
	return nil
}

const maxRetainedCatalogBuffer = 1 << 20

var catalogBuffers = sync.Pool{New: func() any { return new(bytes.Buffer) }}

func writeGatewayCatalog(w http.ResponseWriter, response gatewayCatalogResponse) error {
	body := catalogBuffers.Get().(*bytes.Buffer)
	body.Reset()
	defer func() {
		if body.Cap() <= maxRetainedCatalogBuffer {
			body.Reset()
			catalogBuffers.Put(body)
		}
	}()
	if err := encodeGatewayCatalog(body, response); err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(body.Len()))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body.Bytes())
	return nil
}
