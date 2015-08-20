// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
// Rewritten of lbsas.tcp, will replace it in the near future
// History:
// 2015-08-17	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package tcp2

import (
	"encoding/hex"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"lbsas/utils"
	"net"
	"net/http"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

const (
	MAX_PACKET_LEN = 260
)

// globals
var gEnv *EnviromentCfg = nil
var gProtoList []dbh.IGPSProto = nil
var gDBHelper *dbh.DbHelper = nil

type TCPServer struct {
}

// main
func New(env EnviromentCfg) *TCPServer {
	log.SetLevel(env.LogLevel)
	log.SetFormatter(&log.TextFormatter{})
	var err error = nil
	if gEnv == nil {
		gEnv = &env
	}

	if gDBHelper == nil {
		gDBHelper, err = dbh.New(env)
		if err != nil {
			log.Error(err)
			return nil
		}
	}

	ret := &TCPServer{}

	go func() {
		a, e := net.ResolveTCPAddr("tcp", env.TCPAddr)
		if e != nil {
			log.Fatal(e)
		}
		l, e := net.ListenTCP("tcp", a)
		if e != nil {
			log.Panic(e)
		}
		//
		defer l.Close()
		defer gDBHelper.Close()

		for {
			c, e := l.Accept()
			if e != nil {
				log.Error(e)
				continue
			}
			go tcpStartSession(c)
		}
	}()

	// start the embedded web server
	r := mux.NewRouter()
	r.HandleFunc("/api/{component}", ret._apiHandlerTcp)
	http.Handle("/", r)
	go http.ListenAndServe(gEnv.HTTPAddr, nil)

	return ret
}

func Register(v dbh.IGPSProto) {
	log.Debug("gprotolist: ", gProtoList, "len:", len(gProtoList), ", cap: ", cap(gProtoList))
	gProtoList = append(gProtoList, v)
	log.Debug("registered: ", v)
}

// tcp session handler
func tcpStartSession(conn net.Conn) {
	defer conn.Close()
	packetsChan := make(chan dbh.IGPSProto, gEnv.QueueSizePerConn)
	defer close(packetsChan)

	var proto dbh.IGPSProto = nil

	// create a default worker
	go tcpWorker(packetsChan)

	var (
		last, n int
		err     error
		buff    []byte
	)

	last, n, err, buff =
		0, 0, nil,
		make([]byte, MAX_PACKET_LEN)

	// block readings on the tcp socket
	for {
		// set read timeout
		conn.SetReadDeadline(time.Now().Add(time.Duration(gEnv.TCPTimeOutSec) * time.Second))
		n, err = conn.Read(buff[last:])
		if err != nil {
			break
		}
		// for terminal test
		for i := 0; i < 2; i++ {
			if last+n > 0 {
				if buff[last+n-1] == 0x0a || buff[last+n-1] == 0x0d {
					buff[last+n-1] = 0
					n--
				}
			}
		}
		if n == 0 {
			log.Debug("empty packet, continue")
			continue
		}
		//
		if proto == nil {
			for k, v := range gProtoList {
				if v == nil {
					log.Error("nil proto ", k)
					continue
				}
				log.Debug("v: ", v)
				t := v.New(buff[:last+n])
				if t.IsValid() {
					proto = t
					break
				}
			}
		} else {
			proto = proto.New(buff[:last+n])
		}

		if proto == nil {
			log.Error("protocol not supported", hex.EncodeToString(buff[:last+n]))
			return
		}

		whole := proto.IsWhole()
		if !whole {
			last += n
			log.Debug("not whole packet:", proto)
		} else {
			select {
			case packetsChan <- proto:
			default:
				<-packetsChan
				packetsChan <- proto
				log.Error("Receiv buff overflow. From:", conn.RemoteAddr(), ", proto: ", proto)
			}
		}
	}

	// teardown
	// we are not interested in EOF
	if err != nil && err.Error() != "EOF" {
		// aliyun finance ECS is always connecting ports shortly to check status
		log.Debug(err)
	}
}

// code for statistics, just skip it
func (s *TCPServer) _apiHandlerTcp(w http.ResponseWriter, r *http.Request) {
	var ret []byte
	vars := mux.Vars(r)
	coapi := vars["component"]
	switch coapi {
	case "tcpstatus":
		ret = []byte("{\"success\":true, \"msg\":\"loglevel set success\"}")
	case "set":
		lvl, err := utils.String2LogLevel(r.FormValue("loglevel"))
		if err == nil {
			log.SetLevel(lvl)
		}
		ret = []byte("{\"success\":true, \"msg\":\"loglevel set success\"}")

	default:
		ret = []byte("{\"success\":false, \"msg\":\"unknown api\"}")

	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(ret)
}

func tcpWorker(packetsChan chan dbh.IGPSProto) {
	for {
		proto := <-packetsChan
		if proto == nil {
			return
		}
		if proto.HandleMsg() {
			for {
				select {
				case gDBHelper.DBMsgChan <- proto:
					log.Debug("inserted in to dbcache: ", proto)
					goto BREAK_
				default:
					<-gDBHelper.DBMsgChan
					log.Warn("DBMsgChan overflow")
				}
			}
		BREAK_:
		}
	}
}

func init() {
	gProtoList = make([]dbh.IGPSProto, 0)
	log.Debug("framework inited")
}
