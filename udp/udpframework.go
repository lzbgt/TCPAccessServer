// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu  Initial version
//

package udp

import (
	"fmt"
	. "lbsas/datatypes"
	"lbsas/vendors/ty905"
	"net"
	"net/http"

	log "github.com/Sirupsen/logrus"

	"github.com/gorilla/mux"
)

type UDPServer struct {
	env EnviromentCfg
}

func New(env EnviromentCfg) *UDPServer {
	ret := &UDPServer{env}
	udpAddr, err := net.ResolveUDPAddr("udp", ret.env.TCPAddr)
	if err != nil {
		log.Error(err)
		return nil
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Error(err)
		return nil
	}

	var packetsChan = make(chan *RawUdpPacket, ret.env.MsgCacheSize)

	for i := 0; i < ret.env.NumUDPWokers; i++ {
		go worker(packetsChan)
	}

	// !!! MAIN !!!
	go func() {
		for {
			var rawPacket RawUdpPacket
			// fmt.Println("waiting packets...")
			n, remote, err := udpConn.ReadFromUDP(rawPacket.Buff[0:])
			if err != nil {
				fmt.Println("Error Reading")
			} else {
				rawPacket.Size = n
				rawPacket.Remote = remote
				rawPacket.UdpConn = udpConn
				select {
				case packetsChan <- &rawPacket:
				default:
					<-packetsChan
					packetsChan <- &rawPacket
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

func worker(packetsChan chan *RawUdpPacket) {
	for {
		rawPacket := <-packetsChan
		ty905.New(rawPacket).Srv()
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
