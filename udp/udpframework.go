// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu  Initial version
//

package udp

import (
	"fmt"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"lbsas/vendors/ty905"
	"net"
	"net/http"

	log "github.com/Sirupsen/logrus"

	"github.com/gorilla/mux"
)

var GProtoList []dbh.IGPSProto = nil
var DBHelper *dbh.DbHelper = nil

type UDPServer struct {
	Env EnviromentCfg
}

var _udpServer *UDPServer = nil

func New(env EnviromentCfg) *UDPServer {
	if _udpServer != nil {
		return _udpServer
	}

	var err error = nil
	DBHelper, err = dbh.New(env)
	if err != nil {
		log.Error(err)
		return nil
	}

	//
	GProtoList = []dbh.IGPSProto{ty905.New(RawUdpPacket{})}
	_udpServer = &UDPServer{env}
	ret := _udpServer
	udpAddr, err := net.ResolveUDPAddr("udp", ret.Env.TCPAddr)
	if err != nil {
		log.Error(err)
		return nil
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Error(err)
		return nil
	}

	var packetsChan = make(chan RawUdpPacket, ret.Env.MsgCacheSize)

	for i := 0; i < ret.Env.NumUDPWokers; i++ {
		go worker(packetsChan)
	}

	log.Info("dbcache size:", ret.Env.MsgCacheSize, " udp worker num:", ret.Env.NumUDPWokers)

	// !!! MAIN !!!
	go func() {
		for {
			var rawPacket RawUdpPacket
			rawPacket.Buff = make([]byte, 160)
			fmt.Println("waiting packets...")
			_, remote, err := udpConn.ReadFromUDP(rawPacket.Buff)
			if err != nil {
				fmt.Println("Error Reading")
			} else {
				rawPacket.Remote = remote
				rawPacket.UdpConn = udpConn
				select {
				case packetsChan <- rawPacket:
				default:
					<-packetsChan
					packetsChan <- rawPacket
				}
			}
		}
	}()

	// start the embedded web server
	r := mux.NewRouter()
	r.HandleFunc("/api/{component}", _apiHandler)
	http.Handle("/", r)
	go http.ListenAndServe("127.0.0.1:9090", nil)
	return ret
}

func worker(packetsChan chan RawUdpPacket) {
	for {
		rawPacket := <-packetsChan
		for _, v := range GProtoList {
			t := v.New(rawPacket)
			if t.IsValid() {
				if t.HandleMsg() {
					for {
						select {
						case DBHelper.DBMsgChan <- t:
							log.Debug("inserted in to dbcache: ", t)
							goto BREAK_
						default:
							// database pipe overflow, pop the oldest one and insert the new one
							<-DBHelper.DBMsgChan
						}
					}
				BREAK_:
				}
				break
			}
		}
	}
}

func _apiHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coapi := vars["component"]
	var ret []byte
	switch coapi {
	case "stats":
	default:
		ret = []byte("{\"failed\":true, \"msg\":\"unknown api\"}")

	}
	fmt.Println("web")

	w.Header().Set("Content-Type", "application/json")
	w.Write(ret)
}
