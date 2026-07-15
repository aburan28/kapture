// Package feedbridge adapts the ABI feed stream into the replay.Reader
// interface, so engine adapters can reuse components built on Reader (the
// HTTP feed server, most notably) without touching storage themselves.
package feedbridge

import (
	"context"
	"io"

	"github.com/kapture-io/kapture/internal/plugin/replay"
	"github.com/kapture-io/kapture/internal/storage"
	replayenginev1 "github.com/kapture-io/kapture/proto/replayengine/v1"
)

// Reader yields the FeedItems streamed by the host as CapturedRequests.
type Reader struct {
	feed <-chan *replayenginev1.FeedItem
}

// NewReader wraps an ABI feed channel.
func NewReader(feed <-chan *replayenginev1.FeedItem) *Reader {
	return &Reader{feed: feed}
}

func (r *Reader) Open(_ context.Context, _ replay.ReadOptions) error { return nil }

func (r *Reader) Next(ctx context.Context) (*storage.CapturedRequest, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case item, ok := <-r.feed:
		if !ok {
			return nil, io.EOF
		}
		return capturedFromItem(item), nil
	}
}

func (r *Reader) Close() error { return nil }

func capturedFromItem(item *replayenginev1.FeedItem) *storage.CapturedRequest {
	req := &storage.CapturedRequest{
		ID:       item.RequestId,
		Method:   item.Method,
		Path:     item.Path,
		Body:     item.Body,
		Protocol: item.Protocol,
		Metadata: item.Metadata,
	}
	if item.CapturedAt != nil {
		req.Timestamp = item.CapturedAt.AsTime()
	}
	if len(item.Headers) > 0 {
		req.Headers = make(map[string][]string, len(item.Headers))
		for k, hv := range item.Headers {
			req.Headers[k] = hv.GetValues()
		}
	}
	return req
}

var _ replay.Reader = (*Reader)(nil)
