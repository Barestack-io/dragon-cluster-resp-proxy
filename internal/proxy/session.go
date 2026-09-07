package proxy

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/cluster"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/command"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/config"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/metrics"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/pool"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/pubsub"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
)

type session struct {
	cfg         config.Config
	router      *cluster.Router
	metrics     *metrics.Metrics
	log         *slog.Logger
	conn        net.Conn
	r           *resp.Reader
	w           *bufio.Writer
	enc         []byte
	requireAuth bool
	authed      bool
	clientPass  string
	quit        bool
	db          int

	multi   bool
	queued  [][][]byte
	watched [][]byte
	pin     *pool.Conn
	pinPool *pool.Pool
	pinGen  uint64

	sub     *pubsub.Session
	pushCh  chan resp.Value
	writeMu sync.Mutex
}

func newSession(cfg config.Config, r *cluster.Router, m *metrics.Metrics, log *slog.Logger, c net.Conn) *session {
	s := &session{
		cfg:         cfg,
		router:      r,
		metrics:     m,
		log:         log,
		conn:        c,
		r:           resp.NewReaderSize(c, 256*1024),
		w:           bufio.NewWriterSize(c, 256*1024),
		enc:         make([]byte, 0, 256),
		requireAuth: cfg.ClientAuthPassword != "",
		authed:      cfg.ClientAuthPassword == "",
		clientPass:  cfg.ClientAuthPassword,
	}
	return s
}

func (s *session) run(ctx context.Context) {
	defer s.cleanup()
	for !s.quit {
		if err := ctx.Err(); err != nil {
			return
		}
		_ = s.conn.SetReadDeadline(time.Now().Add(s.cfg.ReadTimeout * 4))
		v, err := s.r.ReadValue()
		if err != nil {
			if err != io.EOF {
				s.log.DebugContext(ctx, "client read ended", "error", err)
			}
			return
		}
		argv, err := resp.ArrayToArgv(v)
		if err != nil {
			_ = s.reply(resp.Error("ERR protocol error"))
			return
		}
		name := "unknown"
		if len(argv) > 0 {
			name = upperName(argv[0])
		}
		batch, err := s.drainBatch(clientCmd{argv: argv, raw: v.Raw, name: name})
		if err != nil {
			if err != io.EOF {
				s.log.DebugContext(ctx, "session error", "error", err)
			}
			return
		}
		if err := s.handleBatch(ctx, batch); err != nil {
			s.log.DebugContext(ctx, "session error", "error", err)
			return
		}
	}
}

func (s *session) handle(ctx context.Context, argv [][]byte, raw []byte) error {
	start := time.Now()
	name := "unknown"
	if len(argv) > 0 {
		name = upperName(argv[0])
	}
	result := "ok"

	if s.sub != nil {
		err := s.handleSub(ctx, argv)
		s.observe(name, result, start)
		return err
	}

	if name == "SELECT" {
		err := s.cmdSelect(argv)
		s.observe(name, result, start)
		return err
	}

	if local, ok := handleLocal(s, argv); ok {
		if local.IsError() {
			result = "error"
		}
		s.observe(name, result, start)
		return s.reply(local)
	}

	if name == "MULTI" {
		s.observe(name, result, start)
		return s.cmdMulti()
	}
	if name == "DISCARD" {
		s.observe(name, result, start)
		return s.cmdDiscard()
	}
	if name == "EXEC" {
		err := s.cmdExec(ctx)
		s.observe(name, result, start)
		return err
	}
	if name == "WATCH" {
		err := s.cmdWatch(ctx, argv)
		s.observe(name, result, start)
		return err
	}
	if name == "UNWATCH" {
		err := s.cmdUnwatch(ctx)
		s.observe(name, result, start)
		return err
	}

	if s.multi {
		s.queued = append(s.queued, cloneArgv(argv))
		s.observe(name, result, start)
		return s.reply(queued)
	}

	op := command.ClassifyPubSub(argv[0])
	switch op {
	case command.PubSubSubscribe, command.PubSubPSubscribe, command.PubSubSSubscribe:
		return s.enterSub(ctx, argv, name, start)
	case command.PubSubPublish, command.PubSubSPublish:
		return s.cmdPublish(ctx, argv, name, start)
	}

	if name == "SWAPDB" {
		s.observe(name, "error", start)
		return s.reply(errSwapDB)
	}
	if name == "FLUSHALL" {
		s.observe(name, "error", start)
		return s.reply(errFlushAll)
	}
	if name == "FLUSHDB" {
		err := s.cmdFlushDB(ctx)
		s.observe(name, result, start)
		return err
	}
	if name == "MOVE" {
		err := s.cmdMove(ctx, argv)
		s.observe(name, result, start)
		return err
	}
	if name == "COPY" {
		err := s.cmdCopy(ctx, argv)
		s.observe(name, result, start)
		return err
	}
	if name == "RANDOMKEY" {
		err := s.cmdRandomKey(ctx)
		s.observe(name, result, start)
		return err
	}

	rewritten := command.RewriteArgv(argv, s.db)
	if s.db > 0 {
		argv = rewritten
		raw = nil
	}

	keys := command.ExtractKeys(argv)
	if len(keys) > 1 && s.router.CrossSlot(keys) {
		s.observe(name, "error", start)
		return s.reply(errCross)
	}

	var slot uint16
	if len(keys) > 0 {
		slot = s.router.SlotOf(keys[0])
	} else if n, ok := s.router.AnyMaster(); ok {
		_ = n
		slot = 0
	}

	readTO := s.cfg.ReadTimeout
	blocking := command.IsBlocking(argv)
	if blocking {
		readTO = s.cfg.BlockingMaxWait
	}
	read := command.KindOf(argv[0]) == command.KindRead

	var reply resp.Value
	var err error
	if blocking {
		reply, err = s.doBlocking(ctx, slot, argv, readTO)
	} else if len(raw) > 0 && !needsRewrite(argv) {
		reply, err = s.router.ExecRaw(ctx, slot, raw, read, readTO)
	} else {
		reply, err = s.router.Exec(ctx, slot, argv, read, readTO)
	}
	if err != nil {
		s.observe(name, "error", start)
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	if reply.IsError() {
		result = classifyErr(reply)
		if reply.EqualKind("MOVED") || reply.EqualKind("ASK") {
			// Should have been followed; still hide from client.
			s.observe(name, result, start)
			return s.reply(resp.Error("ERR backend redirect exhausted"))
		}
	} else {
		reply = command.StripReplyKeys(name, s.db, reply)
	}
	s.observe(name, result, start)
	return s.reply(reply)
}

func needsRewrite(argv [][]byte) bool {
	return command.ClassifyPubSub(argv[0]) == command.PubSubPublish
}

func classifyErr(v resp.Value) string {
	switch {
	case v.EqualKind("MOVED"):
		return "moved"
	case v.EqualKind("ASK"):
		return "ask"
	case v.EqualKind("LOADING"):
		return "error"
	default:
		return "error"
	}
}

func (s *session) doBlocking(ctx context.Context, slot uint16, argv [][]byte, readTO time.Duration) (resp.Value, error) {
	c, p, err := s.router.Checkout(ctx, slot)
	if err != nil {
		return resp.Value{}, err
	}
	v, err := c.Do(argv, s.cfg.WriteTimeout, readTO)
	if err != nil {
		p.Drop(c)
		return resp.Value{}, err
	}
	if v.IsError() {
		if red, ok := cluster.ParseRedirect(v.Str); ok && red.Kind == "MOVED" {
			p.Drop(c)
			s.router.Slots().UpdateSlot(red.Slot, red.Addr, "")
			return s.router.Exec(ctx, red.Slot, argv, false, readTO)
		}
	}
	p.Put(c)
	return v, nil
}

func (s *session) cmdMulti() error {
	if s.multi {
		return s.reply(errNested)
	}
	s.multi = true
	s.queued = s.queued[:0]
	return s.reply(resp.SimpleString("OK"))
}

func (s *session) cmdDiscard() error {
	if !s.multi {
		return s.reply(resp.Error("ERR DISCARD without MULTI"))
	}
	s.multi = false
	s.queued = nil
	return s.reply(resp.SimpleString("OK"))
}

func (s *session) cmdSelect(argv [][]byte) error {
	if !s.authed {
		return s.reply(errNoAuth)
	}
	if len(argv) != 2 {
		return s.reply(resp.Error("ERR wrong number of arguments for 'select' command"))
	}
	db, err := command.ParseDBIndex(argv[1], s.cfg.MaxDatabases)
	if err != nil {
		return s.reply(command.DBIndexError(err))
	}
	if s.multi {
		s.queued = append(s.queued, cloneArgv(argv))
		return s.reply(queued)
	}
	s.db = db
	return s.reply(resp.SimpleString("OK"))
}

func (s *session) cmdWatch(ctx context.Context, argv [][]byte) error {
	if s.multi {
		return s.reply(errWatchTxn)
	}
	argv = command.RewriteArgv(argv, s.db)
	keys := command.ExtractKeys(argv)
	if len(keys) == 0 {
		return s.reply(resp.Error("ERR wrong number of arguments for 'watch' command"))
	}
	if s.router.CrossSlot(keys) {
		return s.reply(errCross)
	}
	slot := s.router.SlotOf(keys[0])
	if s.pin == nil {
		c, p, err := s.router.Checkout(ctx, slot)
		if err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
		s.pin, s.pinPool = c, p
		s.pinGen = s.router.Generation()
	}
	v, err := s.pin.Do(argv, s.cfg.WriteTimeout, s.cfg.ReadTimeout)
	if err != nil {
		s.dropPin()
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	s.watched = append(s.watched, cloneArgv(keys)...)
	return s.reply(v)
}

func (s *session) cmdUnwatch(ctx context.Context) error {
	_ = ctx
	if s.pin != nil {
		_, _ = s.pin.Do([][]byte{[]byte("UNWATCH")}, s.cfg.WriteTimeout, s.cfg.ReadTimeout)
		s.dropPin()
	}
	s.watched = nil
	return s.reply(resp.SimpleString("OK"))
}

func (s *session) cmdExec(ctx context.Context) error {
	if !s.multi {
		return s.reply(errNoMulti)
	}
	s.multi = false
	queued := s.queued
	s.queued = nil
	if s.pin != nil && s.pinGen != s.router.Generation() {
		s.dropPin()
		return s.reply(resp.Error("EXECABORT Transaction discarded because of topology change"))
	}
	db := s.db
	items := make([]execItem, 0, len(queued))
	var keys [][]byte
	keys = append(keys, s.watched...)
	for _, a := range queued {
		if len(a) > 0 && upperName(a[0]) == "SELECT" {
			if len(a) != 2 {
				items = append(items, execItem{local: resp.Error("ERR wrong number of arguments for 'select' command")})
				continue
			}
			n, err := command.ParseDBIndex(a[1], s.cfg.MaxDatabases)
			if err != nil {
				items = append(items, execItem{local: command.DBIndexError(err)})
				continue
			}
			db = n
			items = append(items, execItem{local: resp.SimpleString("OK")})
			continue
		}
		rewritten := command.RewriteArgv(a, db)
		items = append(items, execItem{argv: rewritten})
		keys = append(keys, command.ExtractKeys(rewritten)...)
	}
	if len(keys) > 1 && s.router.CrossSlot(keys) {
		s.dropPin()
		return s.reply(errCross)
	}
	var backend [][][]byte
	for _, it := range items {
		if it.argv != nil {
			backend = append(backend, it.argv)
		}
	}
	if len(backend) == 0 {
		s.db = db
		s.watched = nil
		out := make([]resp.Value, len(items))
		for i, it := range items {
			out[i] = it.local
		}
		return s.reply(resp.Array(out...))
	}
	var slot uint16
	if len(keys) > 0 {
		slot = s.router.SlotOf(keys[0])
	}
	c, p := s.pin, s.pinPool
	s.pin, s.pinPool = nil, nil
	if c == nil {
		var err error
		c, p, err = s.router.Checkout(ctx, slot)
		if err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
	}
	defer func() {
		if p != nil && c != nil {
			p.Put(c)
		}
	}()

	if err := c.WriteRaw(resp.EncodeCommand(nil, [][]byte{[]byte("MULTI")}), s.cfg.WriteTimeout); err != nil {
		p.Drop(c)
		c, p = nil, nil
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	if _, err := c.Read(s.cfg.ReadTimeout); err != nil {
		p.Drop(c)
		c, p = nil, nil
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	for _, a := range backend {
		if err := c.WriteRaw(resp.EncodeCommand(nil, a), s.cfg.WriteTimeout); err != nil {
			p.Drop(c)
			c, p = nil, nil
			return s.reply(resp.Error("ERR " + err.Error()))
		}
		if _, err := c.Read(s.cfg.ReadTimeout); err != nil {
			p.Drop(c)
			c, p = nil, nil
			return s.reply(resp.Error("ERR " + err.Error()))
		}
	}
	v, err := c.Do([][]byte{[]byte("EXEC")}, s.cfg.WriteTimeout, s.cfg.ReadTimeout)
	if err != nil {
		p.Drop(c)
		c, p = nil, nil
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	s.watched = nil
	if v.Null {
		return s.reply(v)
	}
	s.db = db
	return s.reply(mergeExecReply(items, v))
}

type execItem struct {
	local resp.Value
	argv  [][]byte
}

func mergeExecReply(items []execItem, v resp.Value) resp.Value {
	if v.Type != resp.TypeArray || v.Null {
		return v
	}
	out := make([]resp.Value, len(items))
	bi := 0
	for i, it := range items {
		if it.argv == nil {
			out[i] = it.local
			continue
		}
		if bi < len(v.Values) {
			out[i] = v.Values[bi]
			bi++
			continue
		}
		out[i] = resp.Error("ERR EXEC reply missing")
	}
	return resp.Array(out...)
}

func (s *session) cmdMove(ctx context.Context, argv [][]byte) error {
	if len(argv) != 3 {
		return s.reply(resp.Error("ERR wrong number of arguments for 'move' command"))
	}
	destDB, err := command.ParseDBIndex(argv[2], s.cfg.MaxDatabases)
	if err != nil {
		return s.reply(command.DBIndexError(err))
	}
	if destDB == s.db {
		return s.reply(resp.Error("ERR source and destination objects are the same"))
	}
	src := command.PrefixKey(s.db, argv[1])
	dst := command.PrefixKey(destDB, argv[1])
	n, err := s.transferKey(ctx, src, dst, true, false)
	if err != nil {
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	return s.reply(resp.Integer(n))
}

func (s *session) cmdCopy(ctx context.Context, argv [][]byte) error {
	if len(argv) < 3 {
		return s.reply(resp.Error("ERR wrong number of arguments for 'copy' command"))
	}
	destDB := s.db
	replace := false
	for i := 3; i < len(argv); i++ {
		if resp.EqualFoldASCII(argv[i], []byte("REPLACE")) {
			replace = true
			continue
		}
		if resp.EqualFoldASCII(argv[i], []byte("DB")) {
			if i+1 >= len(argv) {
				return s.reply(resp.Error("ERR syntax error"))
			}
			n, err := command.ParseDBIndex(argv[i+1], s.cfg.MaxDatabases)
			if err != nil {
				return s.reply(command.DBIndexError(err))
			}
			destDB = n
			i++
		}
	}
	src := command.PrefixKey(s.db, argv[1])
	dst := command.PrefixKey(destDB, argv[2])
	n, err := s.transferKey(ctx, src, dst, false, replace)
	if err != nil {
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	return s.reply(resp.Integer(n))
}

func (s *session) transferKey(ctx context.Context, src, dst []byte, delSrc, replace bool) (int64, error) {
	if !replace {
		ex, err := s.router.Exec(ctx, s.router.SlotOf(dst), [][]byte{[]byte("EXISTS"), dst}, true, s.cfg.ReadTimeout)
		if err != nil {
			return 0, err
		}
		if !ex.IsError() && ex.Type == resp.TypeInteger && ex.Int > 0 {
			return 0, nil
		}
	}
	if s.router.SlotOf(src) == s.router.SlotOf(dst) {
		var argv [][]byte
		if delSrc {
			argv = [][]byte{[]byte("RENAME"), src, dst}
		} else {
			argv = [][]byte{[]byte("COPY"), src, dst}
			if replace {
				argv = append(argv, []byte("REPLACE"))
			}
		}
		v, err := s.router.Exec(ctx, s.router.SlotOf(src), argv, false, s.cfg.ReadTimeout)
		if err != nil {
			return 0, err
		}
		if v.IsError() {
			return 0, errFromValue(v)
		}
		if delSrc {
			return 1, nil
		}
		if v.Type == resp.TypeInteger {
			return v.Int, nil
		}
		return 1, nil
	}
	dump, err := s.router.Exec(ctx, s.router.SlotOf(src), [][]byte{[]byte("DUMP"), src}, true, s.cfg.ReadTimeout)
	if err != nil {
		return 0, err
	}
	if dump.IsError() {
		return 0, errFromValue(dump)
	}
	if dump.Null || len(dump.Str) == 0 {
		return 0, nil
	}
	restore := [][]byte{[]byte("RESTORE"), dst, []byte("0"), dump.Str}
	if replace {
		restore = append(restore, []byte("REPLACE"))
	}
	v, err := s.router.Exec(ctx, s.router.SlotOf(dst), restore, false, s.cfg.ReadTimeout)
	if err != nil {
		return 0, err
	}
	if v.IsError() {
		return 0, errFromValue(v)
	}
	if delSrc {
		_, _ = s.router.Exec(ctx, s.router.SlotOf(src), [][]byte{[]byte("DEL"), src}, false, s.cfg.ReadTimeout)
	}
	return 1, nil
}

func errFromValue(v resp.Value) error {
	if len(v.Str) == 0 {
		return &respErr{s: "backend error"}
	}
	return &respErr{s: string(v.Str)}
}

type respErr struct{ s string }

func (e *respErr) Error() string { return e.s }

func (s *session) cmdFlushDB(ctx context.Context) error {
	if s.db == 0 {
		v, err := s.router.Exec(ctx, 0, [][]byte{[]byte("FLUSHDB")}, false, s.cfg.ReadTimeout)
		if err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
		return s.reply(v)
	}
	match := append(command.DBPrefix(s.db), '*')
	for _, addr := range s.router.MasterAddrs() {
		if err := s.scanDeleteOn(ctx, addr, match); err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
	}
	return s.reply(resp.SimpleString("OK"))
}

func (s *session) scanDeleteOn(ctx context.Context, addr string, match []byte) error {
	p := s.router.Pool(addr)
	c, err := p.Get(ctx)
	if err != nil {
		return err
	}
	dropped := false
	defer func() {
		if !dropped {
			p.Put(c)
		}
	}()
	cursor := []byte("0")
	for {
		v, err := c.Do([][]byte{[]byte("SCAN"), cursor, []byte("MATCH"), match, []byte("COUNT"), []byte("256")}, s.cfg.WriteTimeout, s.cfg.ReadTimeout)
		if err != nil {
			p.Drop(c)
			dropped = true
			return err
		}
		if v.IsError() {
			return errFromValue(v)
		}
		if v.Type != resp.TypeArray || len(v.Values) < 2 {
			return nil
		}
		cursor = append([]byte(nil), v.Values[0].Str...)
		keys := v.Values[1]
		if keys.Type == resp.TypeArray && len(keys.Values) > 0 {
			del := make([][]byte, 0, 1+len(keys.Values))
			del = append(del, []byte("DEL"))
			for _, k := range keys.Values {
				if len(k.Str) > 0 {
					del = append(del, k.Str)
				}
			}
			if len(del) > 1 {
				if _, err := c.Do(del, s.cfg.WriteTimeout, s.cfg.ReadTimeout); err != nil {
					p.Drop(c)
					dropped = true
					return err
				}
			}
		}
		if string(cursor) == "0" {
			return nil
		}
	}
}

func (s *session) cmdRandomKey(ctx context.Context) error {
	if s.db == 0 {
		v, err := s.router.Exec(ctx, 0, [][]byte{[]byte("RANDOMKEY")}, true, s.cfg.ReadTimeout)
		if err != nil {
			return s.reply(resp.Error("ERR " + err.Error()))
		}
		if !v.Null && command.HasVirtualDBPrefix(v.Str) {
			return s.reply(resp.NullBulk())
		}
		return s.reply(v)
	}
	match := append(command.DBPrefix(s.db), '*')
	v, err := s.router.Exec(ctx, 0, [][]byte{[]byte("SCAN"), []byte("0"), []byte("MATCH"), match, []byte("COUNT"), []byte("16")}, true, s.cfg.ReadTimeout)
	if err != nil {
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	if v.IsError() {
		return s.reply(v)
	}
	if v.Type == resp.TypeArray && len(v.Values) >= 2 && v.Values[1].Type == resp.TypeArray && len(v.Values[1].Values) > 0 {
		return s.reply(resp.BulkString(command.StripKey(s.db, v.Values[1].Values[0].Str)))
	}
	return s.reply(resp.NullBulk())
}

func (s *session) cmdPublish(ctx context.Context, argv [][]byte, name string, start time.Time) error {
	rewritten, did := pubsub.RewritePublish(s.router.Flavor(), argv)
	if did && s.metrics != nil {
		s.metrics.PubSubRewrites.Inc()
	}
	var slot uint16
	if len(rewritten) > 1 {
		slot = s.router.SlotOf(rewritten[1])
	}
	v, err := s.router.Exec(ctx, slot, rewritten, false, s.cfg.ReadTimeout)
	if err != nil {
		s.observe(name, "error", start)
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	s.observe(name, "ok", start)
	return s.reply(v)
}

func (s *session) enterSub(ctx context.Context, argv [][]byte, name string, start time.Time) error {
	s.pushCh = make(chan resp.Value, 64)
	s.sub = pubsub.NewSession(ctx, s.router, s.metrics, s.cfg.WriteTimeout, s.cfg.ReadTimeout, s.pushCh)
	if s.metrics != nil {
		s.metrics.PubSubSubs.Inc()
	}
	go s.writePushes(ctx)
	replies, err := s.sub.HandleClientCmd(ctx, argv)
	if err != nil {
		s.observe(name, "error", start)
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	s.observe(name, "ok", start)
	for _, r := range replies {
		if err := s.reply(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) handleSub(ctx context.Context, argv [][]byte) error {
	name := upperName(argv[0])
	if name == "PING" {
		if len(argv) == 1 {
			return s.reply(resp.Array(resp.BulkString([]byte("pong")), resp.BulkString([]byte{})))
		}
		return s.reply(resp.Array(resp.BulkString([]byte("pong")), resp.BulkString(argv[1])))
	}
	if name == "QUIT" {
		s.quit = true
		return s.reply(resp.SimpleString("OK"))
	}
	replies, err := s.sub.HandleClientCmd(ctx, argv)
	if err != nil {
		return s.reply(resp.Error("ERR " + err.Error()))
	}
	for _, r := range replies {
		if err := s.reply(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) writePushes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case v, ok := <-s.pushCh:
			if !ok {
				return
			}
			if err := s.reply(v); err != nil {
				return
			}
		}
	}
}

func (s *session) reply(v resp.Value) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.enc = resp.Encode(s.enc[:0], v)
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout)); err != nil {
		return err
	}
	if _, err := s.w.Write(s.enc); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *session) observe(cmd, result string, start time.Time) {
	if s.metrics != nil {
		s.metrics.ObserveCommand(cmd, result, time.Since(start))
	}
}

func (s *session) resetTxn() {
	s.multi = false
	s.queued = nil
	s.watched = nil
	s.dropPin()
}

func (s *session) dropPin() {
	if s.pin != nil && s.pinPool != nil {
		s.pinPool.Drop(s.pin)
	}
	s.pin, s.pinPool = nil, nil
}

func (s *session) cleanup() {
	s.resetTxn()
	if s.sub != nil {
		s.sub.Close()
		if s.metrics != nil {
			s.metrics.PubSubSubs.Dec()
		}
	}
	if s.pushCh != nil {
		close(s.pushCh)
	}
	_ = s.conn.Close()
}

func cloneArgv(in [][]byte) [][]byte {
	out := make([][]byte, len(in))
	for i, a := range in {
		out[i] = append([]byte(nil), a...)
	}
	return out
}
