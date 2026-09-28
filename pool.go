// Package poolleak implements an HTTP handler that reuses request/response
// objects through sync.Pool to avoid per-request allocations.
//
// # Reset contract
//
// Any pooled object MUST be fully reset before it becomes visible to another
// request. Resetting means more than trimming slices: every byte reachable
// through the object (slice backing arrays, map entries) must be cleared,
// because a later user that reuses the backing storage would otherwise be
// able to observe the previous request's data.
//
// The contract is enforced structurally:
//
//   - The sync.Pool variables are package-private, so callers cannot Put
//     objects back behind the package's back.
//   - recycleRequest and (*Handler).recycleResponse are the ONLY funnels that
//     call Put, and each calls Reset first. Normal completion, parse errors,
//     backend errors and canceled contexts all flow through those funnels.
//   - AcquireRequest and AcquireResponse call Reset as well, so an object
//     handed to a request is always zeroed even if a future recycle path is
//     added carelessly.
package poolleak

import "sync"

// Request is the pooled, transport-independent request view.
type Request struct {
	User   string
	Amount int64
	Tags   []string
	Notes  []string
	// Scratch is reusable byte storage for internal encoders.
	Scratch []byte
	// Pad asks encoders to inflate the payload (used to exercise large
	// pooled buffers).
	Pad int

	ctx Done
}

// Done reports whether the request context has been canceled.
type Done interface {
	Done() <-chan struct{}
}

// Context returns the cancellation source associated with the request.
func (r *Request) Context() Done { return r.ctx }

// Response is the pooled response object handed back to callers.
type Response struct {
	Status  int
	Headers map[string]string
	Tags    []string
	// Payload is the rendered response body; its backing array is pooled.
	Payload []byte
	Note    string
	Amount  int64
}

var (
	reqPool  = &sync.Pool{New: func() any { return new(Request) }}
	respPool = &sync.Pool{New: func() any {
		return &Response{Headers: make(map[string]string, 8)}
	}}
)

// AcquireRequest takes a Request from the pool. The returned object is always
// fully reset, regardless of how its previous user recycled it.
func AcquireRequest() *Request {
	r := reqPool.Get().(*Request)
	r.Reset()
	return r
}

// AcquireResponse takes a Response from the pool. The returned object is
// always fully reset, regardless of how its previous user recycled it.
func AcquireResponse() *Response {
	r := respPool.Get().(*Response)
	r.Reset()
	return r
}

// Reset restores a Request to the zero value while keeping its backing
// arrays allocated. Every field that can carry request-scoped data MUST be
// cleared here:
//
//   - User (string header): set to "".
//   - Amount, Pad (scalars): set to zero.
//   - ctx: detached; a context from an old request must never be observed.
//   - Tags, Notes (slices): truncated to length 0. The backing arrays are
//     retained for reuse, so they must not be handed out pre-populated.
//   - Scratch (byte buffer): truncated AND every byte up to cap is zeroed,
//     because stale bytes remain reachable while the array is pooled.
func (r *Request) Reset() {
	r.User = ""
	r.Amount = 0
	r.Pad = 0
	r.ctx = nil

	r.Tags = r.Tags[:0]
	r.Notes = r.Notes[:0]

	clear(r.Scratch[:cap(r.Scratch)])
	r.Scratch = r.Scratch[:0]
}

// Reset restores a Response to the zero value while keeping its backing
// arrays allocated. Every field that can carry response-scoped data MUST be
// cleared here:
//
//   - Status, Amount (scalars): set to zero.
//   - Note (string header): set to "".
//   - Tags: truncated to length 0 (backing array retained).
//   - Headers: all entries deleted; the map itself is reused. Deleting is
//     equivalent to a fresh map for reads and does not reallocate the bucket
//     storage in steady state.
//   - Payload: truncated to length 0 AND every byte up to cap is zeroed.
//     Truncating alone leaves the previous body in the backing array, where
//     a copy/append bug could re-expose it.
func (r *Response) Reset() {
	r.Status = 0
	r.Amount = 0
	r.Note = ""

	r.Tags = r.Tags[:0]

	for k := range r.Headers {
		delete(r.Headers, k)
	}

	clear(r.Payload[:cap(r.Payload)])
	r.Payload = r.Payload[:0]
}

// recycleRequest is the ONLY function allowed to Put into reqPool. It runs on
// every path (success, parse error, backend error, canceled) via defer in
// Handle, so no request can leave a dirty object in the pool.
func recycleRequest(r *Request) {
	r.Reset()
	reqPool.Put(r)
}

// recycleResponse is the ONLY method allowed to Put into respPool. It runs on
// every path (success via Release; parse error, backend error and canceled
// context via the deferred cleanup in Handle), so no response can leave a
// dirty object in the pool.
func (h *Handler) recycleResponse(r *Response) {
	r.Reset()
	respPool.Put(r)
}

// Release returns a successful Response to the pool. Handle contract: when
// Handle returns a non-nil response, the caller MUST call Release exactly
// once after it finishes reading the response; when Handle returns an error,
// the response has already been recycled internally and Release is not
// called.
func (h *Handler) Release(r *Response) {
	h.recycleResponse(r)
}
