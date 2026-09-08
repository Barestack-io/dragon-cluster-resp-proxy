package proxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/command"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

type clientCmd struct {
	argv [][]byte
	raw  []byte
	name string
}

type pipeItem struct {
	cmd      clientCmd
	start    time.Time
	local    resp.Value
	hasLocal bool
	slow     bool
	slot     uint16
	addr     string
	frame    []byte
	argv     [][]byte
	read     bool
	reply    resp.Value
}

func (s *session) maxPipeline() int {
	n := s.cfg.MaxPipeline
	if n < 1 {
		return 128
	}
	return n
}

func (s *session) drainBatch(first clientCmd) ([]clientCmd, error) {
	batch := make([]clientCmd, 0, 16)
	batch = append(batch, first)
	max := s.maxPipeline()
	const coalesce = time.Millisecond
	for len(batch) < max {
		if s.r.BufferedLen() == 0 {
			_ = s.conn.SetReadDeadline(time.Now().Add(coalesce))
			if _, err := s.r.Buffered().Peek(1); err != nil {
				break
			}
		}
		v, err := s.r.ReadValue()
		if err != nil {
			if len(batch) > 0 && isTimeout(err) {
				break
			}
			return batch, err
		}
		argv, err := resp.ArrayToArgv(v)
		if err != nil {
			return batch, err
		}
		name := "unknown"
		if len(argv) > 0 {
			name = upperName(argv[0])
		}
		batch = append(batch, clientCmd{argv: argv, raw: v.Raw, name: name})
	}
	_ = s.conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout * 4))
	return batch, nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func pipelineSlow(name string, argv [][]byte) bool {
	if command.IsBlocking(argv) {
		return true
	}
	switch name {
	case "MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
		"SUBSCRIBE", "SSUBSCRIBE", "PSUBSCRIBE",
		"UNSUBSCRIBE", "SUNSUBSCRIBE", "PUNSUBSCRIBE",
		"PUBLISH", "SPUBLISH", "PUBSUB",
		"MOVE", "COPY", "FLUSHDB", "FLUSHALL", "SWAPDB", "RANDOMKEY",
		"QUIT":
		return true
	default:
		return false
	}
}

func (s *session) handleBatch(ctx context.Context, batch []clientCmd) error {
	if len(batch) == 1 {
		return s.handle(ctx, batch[0].argv, batch[0].raw)
	}
	items := make([]pipeItem, len(batch))
	slow := false
	for i, c := range batch {
		items[i] = s.classifyPipe(c)
		if items[i].slow {
			slow = true
		}
	}
	if slow {
		for _, c := range batch {
			if err := s.handle(ctx, c.argv, c.raw); err != nil {
				return err
			}
		}
		return nil
	}
	return s.execFastPipeline(ctx, items)
}

func (s *session) classifyPipe(c clientCmd) pipeItem {
	it := pipeItem{cmd: c, start: time.Now()}
	if s.sub != nil || pipelineSlow(c.name, c.argv) {
		it.slow = true
		return it
	}
	if c.name == "SELECT" {
		if !s.authed {
			it.local, it.hasLocal = errNoAuth, true
			return it
		}
		if len(c.argv) != 2 {
			it.local, it.hasLocal = resp.Error("ERR wrong number of arguments for 'select' command"), true
			return it
		}
		db, err := command.ParseDBIndex(c.argv[1], s.cfg.MaxDatabases)
		if err != nil {
			it.local, it.hasLocal = command.DBIndexError(err), true
			return it
		}
		s.db = db
		it.local, it.hasLocal = resp.SimpleString("OK"), true
		return it
	}
	if local, ok := handleLocal(s, c.argv); ok {
		it.local, it.hasLocal = local, true
		return it
	}
	argv := c.argv
	raw := c.raw
	if s.db > 0 {
		argv = command.RewriteArgv(argv, s.db)
		raw = nil
	}
	keys := command.ExtractKeys(argv)
	if len(keys) > 1 && s.router.CrossSlot(keys) {
		if command.FanoutOf(argv) != command.FanoutNone {
			it.slow = true
			return it
		}
		it.local, it.hasLocal = errCross, true
		return it
	}
	var slot uint16
	if len(keys) > 0 {
		slot = s.router.SlotOf(keys[0])
	}
	n, ok := s.router.NodeForSlot(slot, false)
	if !ok {
		n, ok = s.router.AnyMaster()
		if !ok {
			it.local, it.hasLocal = resp.Error("ERR no backend nodes"), true
			return it
		}
	}
	frame := raw
	if len(frame) == 0 || needsRewrite(argv) {
		frame = resp.EncodeCommand(nil, argv)
	}
	it.slot = slot
	it.addr = n.Addr
	it.frame = frame
	it.argv = argv
	it.read = command.KindOf(c.argv[0]) == command.KindRead
	return it
}

func (s *session) execFastPipeline(ctx context.Context, items []pipeItem) error {
	type group struct {
		idx []int
	}
	byAddr := make(map[string]*group)
	order := make([]string, 0, 2)
	for i := range items {
		if items[i].hasLocal {
			items[i].reply = items[i].local
			continue
		}
		g, ok := byAddr[items[i].addr]
		if !ok {
			g = &group{}
			byAddr[items[i].addr] = g
			order = append(order, items[i].addr)
		}
		g.idx = append(g.idx, i)
	}

	if len(order) == 1 {
		if err := s.flushGroup(ctx, order[0], items, byAddr[order[0]].idx); err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
	} else if len(order) > 1 {
		errCh := make(chan error, len(order))
		var wg sync.WaitGroup
		for _, addr := range order {
			addr, g := addr, byAddr[addr]
			wg.Go(func() {
				errCh <- s.flushGroup(ctx, addr, items, g.idx)
			})
		}
		wg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil {
				return s.reply(resp.Error("ERR " + err.Error()))
			}
		}
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}
	for i := range items {
		name := items[i].cmd.name
		reply := items[i].reply
		result := "ok"
		if reply.IsError() {
			result = classifyErr(reply)
			if reply.EqualKind("MOVED") || reply.EqualKind("ASK") {
				reply = resp.Error("ERR backend redirect exhausted")
			}
		} else {
			reply = command.StripReplyKeys(name, s.db, reply)
		}
		s.observe(name, result, items[i].start)
		s.enc = resp.Encode(s.enc[:0], reply)
		if _, err := s.w.Write(s.enc); err != nil {
			return err
		}
	}
	return s.w.Flush()
}

func (s *session) flushGroup(ctx context.Context, addr string, items []pipeItem, idx []int) error {
	c, p, err := s.router.CheckoutAddr(ctx, addr)
	if err != nil {
		return err
	}
	n := len(idx)
	payload := make([]byte, 0, 64*n)
	for _, i := range idx {
		payload = append(payload, items[i].frame...)
	}
	if err := c.WriteRaw(payload, s.cfg.WriteTimeout); err != nil {
		p.Drop(c)
		return err
	}
	redirects := 0
	for _, i := range idx {
		v, err := c.Read(s.cfg.ReadTimeout)
		if err != nil {
			p.Drop(c)
			return err
		}
		items[i].reply = v
		if v.IsError() {
			if _, ok := cluster.ParseRedirect(v.Str); ok {
				redirects++
			}
		}
	}
	p.Put(c)
	if redirects == 0 {
		return nil
	}
	for _, i := range idx {
		if !items[i].reply.IsError() {
			continue
		}
		if _, ok := cluster.ParseRedirect(items[i].reply.Str); !ok {
			continue
		}
		v, err := s.router.Exec(ctx, items[i].slot, items[i].argv, items[i].read, s.cfg.ReadTimeout)
		if err != nil {
			return err
		}
		items[i].reply = v
	}
	return nil
}
