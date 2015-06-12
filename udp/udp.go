// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu  Initial version
//

package udp

import (
	"encoding/json"
	"fmt"
	dt "lbsas/datatypes"
	"lbsas/vendors/ty905"
	"net"
	"net/http"
	"time"

	"github.com/gorilla/mux"
)

const (
	_UDP_PORT   = "127.0.0.1:8082"
	_PROTOCOL   = "udp"
	_CHAN_SIZE  = 100000
	_READER_NUM = 100000
)

var GlobalStat, GlobalStatLast struct {
	NumPktsReceived, NumErrorRcv, NumDBMsgStored, NumPktsDroped uint64
	NPRPS, NERPS, NDSPS, NPDPS                                  float32
	LastTime                                                    time.Time
}

func UdpServer() {

	// UDP server
	udpAddr, err := net.ResolveUDPAddr(_PROTOCOL, _UDP_PORT)
	if err != nil {
		fmt.Println("Wrong Address")
		return
	}
	udpConn, err := net.ListenUDP(_PROTOCOL, udpAddr)
	if err != nil {
		fmt.Println(err)
	}

	var packetsChan = make(chan *dt.RawUdpPacket, _CHAN_SIZE)

	for i := 0; i < _READER_NUM; i++ {
		go handlePackets(packetsChan)
	}

	// start packets rcv routine
	go func() {
		for {
			var rawPacket dt.RawUdpPacket
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
					GlobalStat.NumPktsDroped++
				}
				GlobalStat.NumPktsReceived++
			}
		}
	}()

	GlobalStatLast.LastTime = time.Now()

	// start the embedded web server
	r := mux.NewRouter()
	r.HandleFunc("/api/{component}", _apiHandler)
	http.Handle("/", r)
	go http.ListenAndServe("127.0.0.1:9090", nil)

}

func handlePackets(packetsChan chan *dt.RawUdpPacket) {
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
		GlobalStat.LastTime = time.Now()
		// NxxPS
		tdelta := float32(GlobalStat.LastTime.Unix() - GlobalStatLast.LastTime.Unix())
		GlobalStat.NDSPS = (float32)(GlobalStat.NumDBMsgStored-GlobalStatLast.NumDBMsgStored) / tdelta
		GlobalStat.NERPS = (float32)(GlobalStat.NumErrorRcv-GlobalStatLast.NumErrorRcv) / tdelta
		GlobalStat.NPDPS = (float32)(GlobalStat.NumPktsDroped-GlobalStatLast.NumPktsDroped) / tdelta
		GlobalStat.NPRPS = (float32)(GlobalStat.NumPktsReceived-GlobalStatLast.NumPktsReceived) / tdelta

		// NumxxLast
		GlobalStatLast.NumDBMsgStored = GlobalStat.NumDBMsgStored
		GlobalStatLast.NumErrorRcv = GlobalStat.NumErrorRcv
		GlobalStatLast.NumPktsDroped = GlobalStat.NumPktsDroped
		GlobalStatLast.NumPktsReceived = GlobalStat.NumPktsReceived

		//
		GlobalStatLast.LastTime = GlobalStat.LastTime
		ret, _ = json.Marshal(GlobalStat)
	default:
		ret = []byte("{\"failed\":true, \"msg\":\"unknown api\"}")

	}
	fmt.Println("web")

	w.Header().Set("Content-Type", "application/json")
	w.Write(ret)
}
