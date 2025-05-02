package transport

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"
)

type RPC string // RPC identifies the Raft method.

const (
	RPCRequestVote   RPC = "request_vote"
	RPCAppendEntries RPC = "append_entries"
)

// HandlerFunc handles an inbound RPC and returns response or error.
type HandlerFunc func(method RPC, body io.Reader, w http.ResponseWriter)

// HTTPTransport routes JSON‑encoded RPCs over http.Client.
type HTTPTransport struct {
	client  *http.Client
	handler HandlerFunc
}

func New(handler HandlerFunc) *HTTPTransport {
	return &HTTPTransport{
		client:  &http.Client{Timeout: 3 * time.Second},
		handler: handler,
	}
}

func (t *HTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.handler(RPC(r.URL.Path[1:]), r.Body, w)
}

func (t *HTTPTransport) Call(addr string, method RPC, req, resp any) error {
	buf, _ := json.Marshal(req)
	httpResp, err := t.client.Post(
		"http://"+addr+"/"+string(method),
		"application/json",
		bytes.NewReader(buf))

	if err != nil {
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return io.ErrUnexpectedEOF
	}

	return json.NewDecoder(httpResp.Body).Decode(resp)
}

// Utility to reply JSON.
func ReplyJSON(w http.ResponseWriter, v any) {
	data, _ := json.Marshal(v)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}
