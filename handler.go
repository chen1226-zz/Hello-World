package poolleak

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Backend simulates downstream work performed while serving a request.
type Backend interface {
	Process(ctx context.Context, user string, amount int64) error
}

// Handler serves /pay requests using pooled request/response objects.
type Handler struct {
	Backend Backend
}

// NewHandler builds a Handler with a default in-process backend.
func NewHandler() *Handler {
	return &Handler{Backend: nopBackend{}}
}

type nopBackend struct{}

func (nopBackend) Process(context.Context, string, int64) error { return nil }

type sleepingBackend struct{ d time.Duration }

func (b sleepingBackend) Process(ctx context.Context, _ string, _ int64) error {
	select {
	case <-time.After(b.d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// NewSleepingBackend returns a backend that blocks for d and honors context
// cancellation. It is used by the cancellation tests.
func NewSleepingBackend(d time.Duration) Backend { return sleepingBackend{d: d} }

// Handle processes one request and returns a pooled Response.
//
// Ownership: the request is always recycled internally (via a defer, so the
// cancel/error paths cannot skip it). The response ownership is:
//
//   - On success, the caller receives a non-nil *Response and MUST pass it to
//     Release exactly once.
//   - On failure (parse error, backend error, canceled context), Handle
//     itself recycles the response through the same reset-then-Put funnel and
//     returns (nil, err).
func (h *Handler) Handle(r *http.Request) (*Response, error) {
	req := AcquireRequest()
	resp := AcquireResponse()
	defer recycleRequest(req)

	// recycleResp guards every failure branch: whichever path takes the
	// return, the response goes through the single reset-then-Put funnel.
	recycleResp := true
	defer func() {
		if recycleResp {
			h.recycleResponse(resp)
		}
	}()

	if err := parseRequest(r, req); err != nil {
		return nil, err
	}

	if err := h.Backend.Process(r.Context(), req.User, req.Amount); err != nil {
		return nil, err
	}

	if err := h.fill(r.Context(), req, resp); err != nil {
		return nil, err
	}
	resp.Payload = renderPayload(req, resp)

	recycleResp = false // ownership transfers to the caller
	return resp, nil
}

// fill populates resp from req. It returns ctx.Err() when the request is
// canceled while the response is being built; on that path Handle recycles
// the half-built response.
func (h *Handler) fill(ctx context.Context, req *Request, resp *Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	resp.Status = http.StatusOK
	resp.Amount = req.Amount

	// resp.Tags starts at len 0 after Reset; copy tags into the retained
	// backing array instead of appending onto stale entries.
	resp.Tags = append(resp.Tags, req.Tags...)
	if len(req.Notes) > 0 {
		resp.Note = req.Notes[0]
	}

	resp.Headers["X-Canonical"] = canonicalQuery(req)
	if len(resp.Tags) > 0 {
		resp.Headers["X-Tags"] = strings.Join(resp.Tags, ",")
	}
	return nil
}

func parseRequest(r *http.Request, req *Request) error {
	q := r.URL.Query()
	req.User = q.Get("user")

	if raw := q.Get("amount"); raw != "" {
		amount, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("bad amount: %w", err)
		}
		req.Amount = amount
	}

	if t := q.Get("tags"); t != "" {
		// req.Tags is len 0 after Reset, so append fills the retained array
		// from the start; no previous request's tags can survive.
		req.Tags = append(req.Tags, strings.Split(t, ",")...)
	}
	if n := q.Get("notes"); n != "" {
		req.Notes = append(req.Notes, n)
	}
	if pad := q.Get("size"); pad != "" {
		n, err := strconv.Atoi(pad)
		if err != nil {
			return fmt.Errorf("bad size: %w", err)
		}
		req.Pad = n
	}

	req.ctx = r.Context()
	return nil
}

// canonicalQuery builds a stable representation of the request parameters.
// It reuses req.Scratch as a byte buffer. The buffer always starts empty and
// zeroed (see Request.Reset), and append never exposes bytes beyond the
// freshly written length, so no stale prefix/tail can appear in the result.
func canonicalQuery(req *Request) string {
	buf := req.Scratch[:0]
	buf = append(buf, "canonical="...)
	buf = append(buf, req.User...)
	buf = append(buf, '=')
	buf = strconv.AppendInt(buf, req.Amount, 10)
	buf = append(buf, "&key=u:"...)
	req.Scratch = buf
	return string(buf)
}

// renderPayload renders the response body. It appends into resp.Payload,
// which starts empty and fully zeroed after Reset: the returned slice length
// equals the number of freshly written bytes, so bytes from the previous
// response (which still sit in the backing array's tail) are never visible.
func renderPayload(req *Request, resp *Response) []byte {
	buf := resp.Payload[:0]
	buf = append(buf, "user="...)
	buf = append(buf, req.User...)
	buf = append(buf, "&amount="...)
	buf = strconv.AppendInt(buf, req.Amount, 10)
	if resp.Note != "" {
		buf = append(buf, "&note="...)
		buf = append(buf, resp.Note...)
	}
	for _, tag := range resp.Tags {
		buf = append(buf, "&tag="...)
		buf = append(buf, tag...)
	}
	// pad= lets callers inflate the body so the pooled backing array grows;
	// it is fixed padding, never copied from another request's data.
	if req.Pad > 0 {
		buf = append(buf, "&pad="...)
		start := len(buf)
		buf = append(buf, make([]byte, req.Pad)...)
		for i := start; i < len(buf); i++ {
			buf[i] = 'x'
		}
	}
	return buf
}

// ServeHTTP is the net/http entry point.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp, err := h.Handle(r)
	if err != nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(err.Error()))
		return
	}
	defer h.Release(resp)

	for k, v := range resp.Headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Payload)
}
