// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package tcp

import (
	"encoding/json"
	"errors"
	"fmt"
	. "lbsas/datatypes"
	"lbsas/utils"
	"net"
	"net/http"
	"os"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

// globals
type TCPServer struct {
	v                    Vendor
	StatTcp, StatTcpLast TCPStat
	Reportor             *log.Logger
}

// main
func New(v Vendor) *TCPServer {
	reportor := log.New()
	f, err := os.Create("report.log")
	if err != nil {
		log.Fatal(err)
	}
	reportor.Out = f
	log.SetLevel(v.GetCfg().LogLevel)
	log.SetFormatter(&log.TextFormatter{})
	reportor.Level = log.DebugLevel

	ret := &TCPServer{v, TCPStat{}, TCPStat{}, reportor}
	go ret.statusReport()

	// log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	// !!!! MAIN !!!!
	go func() {
		a, e := net.ResolveTCPAddr(v.GetCfg().Protocol, v.GetCfg().Addr)
		if e != nil {
			log.Fatal(e)
		}
		l, e := net.ListenTCP(v.GetCfg().Protocol, a)
		if e != nil {
			log.Panic(e)
		}
		//
		defer l.Close()
		defer v.Close()

		for {
			c, e := l.Accept()
			if e != nil {
				log.Error(e)
				continue
			}
			// new tcp sock session
			ret.StatTcp.NumConnCreated++
			log.Debug("accepted:", c.RemoteAddr().String())
			go ret.tcpStartSession(c)
		}
	}()

	// code for statistics, just skip these
	ret.StatTcp.LastTime = time.Now()
	ret.StatTcp.NowTime = ret.StatTcp.LastTime
	ret.StatTcp.StartTime = ret.StatTcp.LastTime
	ret.StatTcpLast.LastTime = ret.StatTcp.LastTime
	ret.StatTcpLast.NowTime = ret.StatTcp.LastTime
	ret.StatTcpLast.StartTime = ret.StatTcp.LastTime

	// start the embedded web server
	r := mux.NewRouter()
	r.HandleFunc("/api/{component}", ret._apiHandlerTcp)
	http.Handle("/", r)
	go http.ListenAndServe(v.GetCfg().HttpAddr, nil)

	return ret
}

// tcp session handler
func (s *TCPServer) tcpStartSession(conn net.Conn) {
	defer conn.Close()

	// channel to cache packets when the far-end device is busy sending
	packetsChan := make(chan *RawTcpPacket, s.v.GetCfg().ChanSize)
	defer close(packetsChan)

	// create a default worker
	go s.v.TcpWorker(packetsChan)

	var (
		last, n, status int
		whole           bool
		err             error
		buff            []byte
	)

	last, n, err, buff, whole, status =
		0, 0, nil,
		make([]byte, s.v.GetCfg().PacketMaxLen), false, int(0)

	// block readings on the tcp socket
	for {
		// set read timeout
		conn.SetReadDeadline(time.Now().Add(s.v.GetCfg().ReadTimeoutSec * time.Second))
		n, err = conn.Read(buff[last:])
		if err != nil {
			s.StatTcp.NumErrorRcv++
			break
		}

		// TODO: [remove me] for test with netcat/telnet
		// log.Debug("last char:", buff[last+n-1])
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

		// there is remain part unread
		whole, err = s.v.IsWholePacket(buff[last:last+n], &status)
		if err != nil {
			s.StatTcp.NumInvalidPkts++
			// Invalid packet
			err = errors.New(fmt.Sprint(err.Error(), ", Buff:", string(buff[:last+n]),
				", From:", conn.RemoteAddr()))
			break
		} else if !whole {
			last += n
			log.Debug("not whole packet:", string(buff[:last]))
		} else {
			// we got a whole peacket here
			s.StatTcp.NumPktsReceived++
			packet := &RawTcpPacket{make([]byte, last+n), &conn}
			copy(packet.Buff, buff[:last+n])
			// reset counters
			n, last, whole = 0, 0, true

			// insert into the chan
			select {
			case packetsChan <- packet:
			default:
				// TODO: two approches to handle this situation
				// 1) just drop the oldest msg and insert the new one
				// 2) create a now worker on the channel and insert again
				// Currently the first one is applied; the second approche can
				// be a feature in future release, and it's required
				// to release long-time-ide workers [IMPORTANT feature]

				<-packetsChan
				packetsChan <- packet
				s.StatTcp.NumPktsDroped++
				// TODO
				log.Error("Receiv buff overflow. From:", conn.RemoteAddr(), ", Buff size:", s.v.GetCfg().ChanSize)
			}
		}
	}

	// teardown
	// we are not interested in EOF
	if err != nil && err.Error() != "EOF" {
		// aliyun finance ECS always been connected ports shortly to check status
		log.Debug(err)
	}
	s.StatTcp.NumConnClosed++
}

func (s *TCPServer) getStatus() []byte {
	temp := s.StatTcp
	vendorStatTmp := s.v.GetStat()
	var ret []byte

	s.StatTcpLast.LastTime = s.StatTcpLast.NowTime
	s.StatTcpLast.NowTime = time.Now()
	// NxxPS
	tdelta := float32(s.StatTcpLast.NowTime.Unix() - s.StatTcpLast.LastTime.Unix())
	s.StatTcpLast.NumDBMsgStoredPS = (float32)(temp.NumDBMsgStored-s.StatTcpLast.NumDBMsgStored) / tdelta
	s.StatTcpLast.NumErrorRcvPS = (float32)(temp.NumErrorRcv-s.StatTcpLast.NumErrorRcv) / tdelta
	s.StatTcpLast.NumPktsDropedPS = (float32)(temp.NumPktsDroped-s.StatTcpLast.NumPktsDroped) / tdelta
	s.StatTcpLast.NumPktsReceivedPS = (float32)(temp.NumPktsReceived-s.StatTcpLast.NumPktsReceived) / tdelta
	s.StatTcpLast.NumInvalidPktsPS = (float32)(vendorStatTmp.NumInvalidPackets-s.StatTcpLast.NumInvalidPkts) / tdelta
	s.StatTcpLast.NumConnClosedPS = (float32)(temp.NumConnClosed-s.StatTcpLast.NumConnClosed) / tdelta
	s.StatTcpLast.NumConnCreatedPS = (float32)(temp.NumConnCreated-s.StatTcpLast.NumConnCreated) / tdelta
	s.StatTcpLast.NumDBWriteMsgDroppedPS = (float32)(vendorStatTmp.DBWriteMsgDropped-s.StatTcpLast.NumDBWriteMsgDropped) / tdelta

	// max
	//

	// NumxxLast
	s.StatTcpLast.NumDBMsgStored = temp.NumDBMsgStored
	s.StatTcpLast.NumErrorRcv = temp.NumErrorRcv
	s.StatTcpLast.NumPktsDroped = temp.NumPktsDroped
	s.StatTcpLast.NumPktsReceived = temp.NumPktsReceived
	s.StatTcpLast.NumInvalidPkts = vendorStatTmp.NumInvalidPackets
	s.StatTcpLast.NumConnClosed = temp.NumConnClosed
	s.StatTcpLast.NumConnCreated = temp.NumConnCreated
	s.StatTcpLast.NumDBWriteMsgDropped = vendorStatTmp.DBWriteMsgDropped
	s.StatTcpLast.NumDBMsgStored = vendorStatTmp.NumDBMsgStored

	//
	s.StatTcpLast.AvgWorkerTimeMicroSec = vendorStatTmp.AvgWorkerTimeMicroSec
	s.StatTcpLast.AvgDBTimeMicroSec = vendorStatTmp.AvgDBTimeMicroSec
	s.StatTcpLast.NumInvalidPackets = vendorStatTmp.NumInvalidPackets
	s.StatTcpLast.NumDBWriteMsgCacheSize = vendorStatTmp.DBWriteMsgCacheSize
	//
	s.StatTcpLast.NumConnActive = s.StatTcpLast.NumConnCreated - s.StatTcpLast.NumConnClosed

	//
	ret, _ = json.Marshal(s.StatTcpLast)
	return ret
}

// code for statistics, just skip it
func (s *TCPServer) _apiHandlerTcp(w http.ResponseWriter, r *http.Request) {

	var ret []byte
	if w != nil && r != nil {
		vars := mux.Vars(r)
		coapi := vars["component"]
		switch coapi {
		case "tcpstatus":
			ret = s.getStatus()
		case "set":
			lvl, err := utils.String2LogLevel(r.FormValue("loglevel"))
			if err == nil {
				log.SetLevel(lvl)
				s.v.SetLogLevel(lvl)
			}
			ret = []byte("{\"success\":true, \"msg\":\"loglevel set success\"}")

		default:
			ret = []byte("{\"success\":false, \"msg\":\"unknown api\"}")

		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(ret)
	} else {
		// write to database
		s.Reportor.Info(string(s.getStatus()))
	}
}

// timer triggered every 120s to report statistics
func (s *TCPServer) statusReport() {
	s.Reportor.Info("Report starting")
	timeChan := time.NewTicker(time.Second * 120).C
	for {
		<-timeChan
		runtime.ReadMemStats(&s.StatTcpLast.MemStat)
		s._apiHandlerTcp(nil, nil)
	}
	s.Reportor.Info("Report ended")
}
