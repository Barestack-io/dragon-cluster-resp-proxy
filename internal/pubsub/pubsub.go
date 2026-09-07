package pubsub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/command"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/pool"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

// Mode is how we talk pub/sub to the backend.
type Mode uint8

const (
	ModePassthrough Mode = iota
	ModeShardedRewrite
)

// Session multiplexes one client subscribe session onto one or more backend conns.
type Session struct {
	router  *cluster.Router
	metrics *metrics.Metrics
	mode    Mode
	writeTO time.Duration
	readTO  time.Duration

	mu       sync.Mutex
	subs     map[string]string // channel -> backend addr
	backends map[string]*backend
	count    int
	pushCh   chan<- resp.Value
	ctx      context.Context
}

type backend struct {
	addr string
	pool *pool.Pool
	conn *pool.Conn
	acks chan resp.Value
	stop context.CancelFunc
}

// NewSession starts a pub/sub session. pushCh receives rewritten server pushes.
func NewSession(ctx context.Context, r *cluster.Router, m *metrics.Metrics, writeTO, readTO time.Duration, pushCh chan<- resp.Value) *Session {
	mode := ModePassthrough
	if r.Flavor() == cluster.FlavorDragonfly {
		mode = ModeShardedRewrite
	}
	return &Session{
		router:   r,
		metrics:  m,
		mode:     mode,
		writeTO:  writeTO,
		readTO:   readTO,
		subs:     make(map[string]string),
		backends: make(map[string]*backend),
		pushCh:   pushCh,
		ctx:      ctx,
	}
}

// Mode returns the backend pub/sub mode.
func (s *Session) Mode() Mode { return s.mode }

// HandleClientCmd applies a client subscribe-mode command.
func (s *Session) HandleClientCmd(ctx context.Context, argv [][]byte) ([]resp.Value, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty")
	}
	op := command.ClassifyPubSub(argv[0])
	switch op {
	case command.PubSubPSubscribe, command.PubSubPUnsubscribe:
		if s.mode == ModeShardedRewrite {
			return []resp.Value{resp.Error("ERR PSUBSCRIBE is not supported against Dragonfly cluster")}, nil
		}
		return s.roundTrip(ctx, argv)
	case command.PubSubSubscribe, command.PubSubSSubscribe:
		return s.subscribe(ctx, argv[1:], op == command.PubSubSubscribe)
	case command.PubSubUnsubscribe, command.PubSubSUnsubscribe:
		return s.unsubscribe(ctx, argv[1:], op == command.PubSubUnsubscribe)
	default:
		return []resp.Value{resp.Error("ERR only (UN)SUBSCRIBE / PING / QUIT allowed in this mode")}, nil
	}
}

// RewritePublish maps PUBLISH→SPUBLISH on Dragonfly cluster.
func RewritePublish(flavor cluster.Flavor, argv [][]byte) ([][]byte, bool) {
	if flavor != cluster.FlavorDragonfly || len(argv) < 2 {
		return argv, false
	}
	if command.ClassifyPubSub(argv[0]) != command.PubSubPublish {
		return argv, false
	}
	out := make([][]byte, len(argv))
	copy(out, argv)
	out[0] = []byte("SPUBLISH")
	return out, true
}

func (s *Session) subscribe(ctx context.Context, channels [][]byte, fromGlobal bool) ([]resp.Value, error) {
	if len(channels) == 0 {
		return []resp.Value{resp.Error("ERR wrong number of arguments for 'subscribe' command")}, nil
	}
	cmd := []byte("SUBSCRIBE")
	kind := "subscribe"
	if s.mode == ModeShardedRewrite && fromGlobal {
		cmd = []byte("SSUBSCRIBE")
		if s.metrics != nil {
			s.metrics.PubSubRewrites.Inc()
		}
	} else if !fromGlobal {
		cmd = []byte("SSUBSCRIBE")
		kind = "ssubscribe"
	}
	var replies []resp.Value
	for _, ch := range channels {
		slot := s.router.SlotOf(ch)
		n, ok := s.router.NodeForSlot(slot, false)
		if !ok {
			n, ok = s.router.AnyMaster()
			if !ok {
				return []resp.Value{resp.Error("ERR no backend nodes")}, nil
			}
		}
		be, err := s.ensureBackend(ctx, n.Addr)
		if err != nil {
			return []resp.Value{resp.Error("ERR " + err.Error())}, nil
		}
		if err := be.conn.WriteRaw(resp.EncodeCommand(nil, [][]byte{cmd, ch}), s.writeTO); err != nil {
			return []resp.Value{resp.Error("ERR " + err.Error())}, nil
		}
		ack, err := s.waitAck(ctx, be)
		if err != nil {
			return []resp.Value{resp.Error("ERR " + err.Error())}, nil
		}
		s.mu.Lock()
		if _, exists := s.subs[string(ch)]; !exists {
			s.count++
		}
		s.subs[string(ch)] = n.Addr
		cnt := s.count
		s.mu.Unlock()
		replies = append(replies, clientAck(ack, kind, ch, int64(cnt), s.mode == ModeShardedRewrite && fromGlobal))
	}
	return replies, nil
}

func (s *Session) unsubscribe(ctx context.Context, channels [][]byte, fromGlobal bool) ([]resp.Value, error) {
	s.mu.Lock()
	if len(channels) == 0 {
		for ch := range s.subs {
			channels = append(channels, []byte(ch))
		}
	}
	s.mu.Unlock()

	cmd := []byte("UNSUBSCRIBE")
	kind := "unsubscribe"
	if s.mode == ModeShardedRewrite && fromGlobal {
		cmd = []byte("SUNSUBSCRIBE")
	} else if !fromGlobal {
		cmd = []byte("SUNSUBSCRIBE")
		kind = "sunsubscribe"
	}

	var replies []resp.Value
	for _, ch := range channels {
		s.mu.Lock()
		addr := s.subs[string(ch)]
		be := s.backends[addr]
		s.mu.Unlock()
		if be != nil {
			_ = be.conn.WriteRaw(resp.EncodeCommand(nil, [][]byte{cmd, ch}), s.writeTO)
			if ack, err := s.waitAck(ctx, be); err == nil {
				s.mu.Lock()
				delete(s.subs, string(ch))
				if s.count > 0 {
					s.count--
				}
				cnt := s.count
				s.mu.Unlock()
				replies = append(replies, clientAck(ack, kind, ch, int64(cnt), s.mode == ModeShardedRewrite && fromGlobal))
				continue
			}
		}
		s.mu.Lock()
		delete(s.subs, string(ch))
		if s.count > 0 {
			s.count--
		}
		cnt := s.count
		s.mu.Unlock()
		replies = append(replies, resp.Array(
			resp.BulkString([]byte(kind)),
			resp.BulkString(ch),
			resp.Integer(int64(cnt)),
		))
	}
	return replies, nil
}

func (s *Session) roundTrip(ctx context.Context, argv [][]byte) ([]resp.Value, error) {
	n, ok := s.router.AnyMaster()
	if !ok {
		return []resp.Value{resp.Error("ERR no backend nodes")}, nil
	}
	be, err := s.ensureBackend(ctx, n.Addr)
	if err != nil {
		return []resp.Value{resp.Error("ERR " + err.Error())}, nil
	}
	if err := be.conn.WriteRaw(resp.EncodeCommand(nil, argv), s.writeTO); err != nil {
		return []resp.Value{resp.Error("ERR " + err.Error())}, nil
	}
	ack, err := s.waitAck(ctx, be)
	if err != nil {
		return []resp.Value{resp.Error("ERR " + err.Error())}, nil
	}
	return []resp.Value{ack}, nil
}

func (s *Session) waitAck(ctx context.Context, be *backend) (resp.Value, error) {
	t := time.NewTimer(s.readTO)
	defer t.Stop()
	select {
	case v := <-be.acks:
		return v, nil
	case <-ctx.Done():
		return resp.Value{}, ctx.Err()
	case <-t.C:
		return resp.Value{}, errors.New("timeout waiting for subscribe ack")
	}
}

func (s *Session) ensureBackend(ctx context.Context, addr string) (*backend, error) {
	s.mu.Lock()
	if be, ok := s.backends[addr]; ok {
		s.mu.Unlock()
		return be, nil
	}
	s.mu.Unlock()

	c, p, err := s.router.CheckoutAddr(ctx, addr)
	if err != nil {
		return nil, err
	}
	bctx, cancel := context.WithCancel(s.ctx)
	be := &backend{
		addr: addr,
		pool: p,
		conn: c,
		acks: make(chan resp.Value, 16),
		stop: cancel,
	}
	s.mu.Lock()
	s.backends[addr] = be
	s.mu.Unlock()
	go s.forward(bctx, be)
	return be, nil
}

func (s *Session) forward(ctx context.Context, be *backend) {
	for {
		if ctx.Err() != nil {
			return
		}
		v, err := be.conn.Read(s.readTO)
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		if isAck(v) {
			if s.mode == ModeShardedRewrite {
				v = rewriteIncoming(v)
			}
			if isUnsub(v) && s.maybeResub(ctx, v) {
				continue
			}
			select {
			case be.acks <- v:
			case <-ctx.Done():
				return
			}
			continue
		}
		if s.mode == ModeShardedRewrite {
			v = rewriteIncoming(v)
		}
		select {
		case s.pushCh <- v:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Session) maybeResub(ctx context.Context, v resp.Value) bool {
	if len(v.Values) < 2 {
		return false
	}
	ch := string(v.Values[1].Str)
	s.mu.Lock()
	_, tracked := s.subs[ch]
	s.mu.Unlock()
	if !tracked {
		return false
	}
	// Forced unsub after slot migration — resubscribe on the new owner.
	// Must not wait for an ack on this goroutine (it is the reader).
	if s.metrics != nil {
		s.metrics.PubSubResub.Inc()
	}
	go func() {
		_, _ = s.subscribe(ctx, [][]byte{[]byte(ch)}, true)
	}()
	return true
}

func isAck(v resp.Value) bool {
	if (v.Type != resp.TypeArray && v.Type != resp.TypePush) || len(v.Values) == 0 {
		return false
	}
	k := v.Values[0].Str
	return bytes.EqualFold(k, []byte("subscribe")) ||
		bytes.EqualFold(k, []byte("ssubscribe")) ||
		bytes.EqualFold(k, []byte("unsubscribe")) ||
		bytes.EqualFold(k, []byte("sunsubscribe")) ||
		bytes.EqualFold(k, []byte("psubscribe")) ||
		bytes.EqualFold(k, []byte("punsubscribe"))
}

func isUnsub(v resp.Value) bool {
	if len(v.Values) == 0 {
		return false
	}
	k := v.Values[0].Str
	return bytes.EqualFold(k, []byte("unsubscribe")) || bytes.EqualFold(k, []byte("sunsubscribe"))
}

func clientAck(v resp.Value, kind string, ch []byte, count int64, rewrite bool) resp.Value {
	if !rewrite && len(v.Raw) > 0 {
		return v
	}
	return resp.Array(
		resp.BulkString([]byte(kind)),
		resp.BulkString(ch),
		resp.Integer(count),
	)
}

func rewriteIncoming(v resp.Value) resp.Value {
	if (v.Type != resp.TypeArray && v.Type != resp.TypePush) || len(v.Values) == 0 {
		return v
	}
	k := v.Values[0].Str
	switch {
	case bytes.EqualFold(k, []byte("smessage")):
		v.Values[0].Str = []byte("message")
		v.Raw = nil
	case bytes.EqualFold(k, []byte("ssubscribe")):
		v.Values[0].Str = []byte("subscribe")
		v.Raw = nil
	case bytes.EqualFold(k, []byte("sunsubscribe")):
		v.Values[0].Str = []byte("unsubscribe")
		v.Raw = nil
	}
	return v
}

// Close releases backend connections.
func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, be := range s.backends {
		if be.stop != nil {
			be.stop()
		}
		if be.pool != nil && be.conn != nil {
			be.pool.Drop(be.conn)
		}
	}
	s.backends = map[string]*backend{}
	s.subs = map[string]string{}
}

// Count returns the subscription count.
func (s *Session) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}
