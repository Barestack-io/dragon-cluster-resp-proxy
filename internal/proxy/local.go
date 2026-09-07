package proxy

import (
	"bytes"
	"strings"

	"github.com/barestack/dragon-cluster-resp-proxy/internal/resp"
	"github.com/barestack/dragon-cluster-resp-proxy/internal/version"
)

var (
	errCluster  = resp.Error("ERR This instance has cluster support disabled")
	errSwapDB   = resp.Error("ERR SWAPDB is not supported")
	errFlushAll = resp.Error("ERR FLUSHALL is not supported; use FLUSHDB on a virtual database")
	errAuth     = resp.Error("WRONGPASS invalid username-password pair or user is disabled.")
	errNoAuth   = resp.Error("NOAUTH Authentication required.")
	errCross    = resp.Error("CROSSSLOT Keys in request don't hash to the same slot")
	errNoMulti  = resp.Error("ERR EXEC without MULTI")
	errNested   = resp.Error("ERR MULTI calls can not be nested")
	errWatchTxn = resp.Error("ERR WATCH inside MULTI is not allowed")
	queued      = resp.SimpleString("QUEUED")
	plusPong    = resp.SimpleString("PONG")
)

func handleLocal(s *session, argv [][]byte) (resp.Value, bool) {
	if len(argv) == 0 {
		return resp.Value{}, false
	}
	name := upperName(argv[0])
	switch name {
	case "PING":
		if !s.authed {
			return errNoAuth, true
		}
		if len(argv) == 1 {
			return plusPong, true
		}
		return resp.BulkString(argv[1]), true
	case "ECHO":
		if !s.authed {
			return errNoAuth, true
		}
		if len(argv) != 2 {
			return resp.Error("ERR wrong number of arguments for 'echo' command"), true
		}
		return resp.BulkString(argv[1]), true
	case "AUTH":
		return s.doAuth(argv), true
	case "QUIT":
		s.quit = true
		return resp.SimpleString("OK"), true
	case "CLUSTER":
		if !s.authed {
			return errNoAuth, true
		}
		return handleCluster(argv), true
	case "COMMAND":
		return resp.Value{}, false
	case "INFO":
		if !s.authed {
			return errNoAuth, true
		}
		return infoReply(), true
	case "HELLO":
		if !s.authed && s.requireAuth {
			// HELLO can include AUTH
			if v, ok := s.helloAuth(argv); ok {
				return v, true
			}
			return errNoAuth, true
		}
		return helloReply(), true
	case "RESET":
		s.resetTxn()
		s.db = 0
		s.authed = !s.requireAuth
		return resp.SimpleString("OK"), true
	default:
		if s.requireAuth && !s.authed {
			return errNoAuth, true
		}
		return resp.Value{}, false
	}
}

func (s *session) doAuth(argv [][]byte) resp.Value {
	if !s.requireAuth {
		return resp.SimpleString("OK")
	}
	var pass []byte
	switch len(argv) {
	case 2:
		pass = argv[1]
	case 3:
		pass = argv[2]
	default:
		return resp.Error("ERR wrong number of arguments for 'auth' command")
	}
	if string(pass) == s.clientPass {
		s.authed = true
		return resp.SimpleString("OK")
	}
	return errAuth
}

func (s *session) helloAuth(argv [][]byte) (resp.Value, bool) {
	for i := 1; i < len(argv)-1; i++ {
		if bytes.EqualFold(argv[i], []byte("AUTH")) {
			if i+2 >= len(argv) {
				return errAuth, true
			}
			if string(argv[i+2]) == s.clientPass {
				s.authed = true
				return helloReply(), true
			}
			return errAuth, true
		}
	}
	return resp.Value{}, false
}

func handleCluster(argv [][]byte) resp.Value {
	if len(argv) < 2 {
		return errCluster
	}
	sub := strings.ToUpper(string(argv[1]))
	switch sub {
	case "HELP":
		return resp.Array(
			resp.BulkString([]byte("CLUSTER HELP")),
			resp.BulkString([]byte("This instance has cluster support disabled")),
		)
	case "INFO":
		return resp.BulkString([]byte("cluster_state:fail\r\ncluster_slots_assigned:0\r\ncluster_enabled:0\r\n"))
	case "SLOTS", "SHARDS", "NODES":
		return errCluster
	case "KEYSLOT":
		return errCluster
	default:
		return errCluster
	}
}

func infoReply() resp.Value {
	body := strings.Join([]string{
		"# Server",
		"redis_version:7.2.0",
		"redis_mode:standalone",
		"os:proxy",
		"dcrp_version:" + version.Version,
		"cluster_enabled:0",
		"",
	}, "\r\n")
	return resp.BulkString([]byte(body))
}

func helloReply() resp.Value {
	return resp.Array(
		resp.BulkString([]byte("server")), resp.BulkString([]byte("redis")),
		resp.BulkString([]byte("version")), resp.BulkString([]byte("7.2.0")),
		resp.BulkString([]byte("proto")), resp.Integer(2),
		resp.BulkString([]byte("id")), resp.Integer(1),
		resp.BulkString([]byte("mode")), resp.BulkString([]byte("standalone")),
		resp.BulkString([]byte("role")), resp.BulkString([]byte("master")),
		resp.BulkString([]byte("modules")), resp.Array(),
	)
}

func upperName(b []byte) string {
	tmp := make([]byte, len(b))
	return string(resp.UpperASCII(tmp, b))
}
