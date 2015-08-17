// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package nbsihai

import (
	"bytes"
	"fmt"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	gcj "lbsas/gcj02"
	"net"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

// module exported global variables
type NbSiHai struct {
	TcpConfig NetConfig
	*dbh.DbHelper
	Stat VendorStat
}

// module private variables
var _MessageConstants = &struct {
	ClassReport, ClassAT, ClassACK, ClassBuff string
	Commands                                  map[string]interface{}
	Delimiter                                 byte
}{
	"RESP:", "AT+", "ACK:", "BUFF:",
	map[string]interface{}{
		"RESP:GTCTN": MessageResp{},
		"RESP:GTSTR": MessageResp{},
		"RESP:GTRTL": MessageResp{},
		"RESP:GTBL":  MessageResp{},
		"BUFF:GTCTN": MessageResp{},
		"BUFF:GTSTR": MessageResp{},
		"BUFF:GTRTL": MessageResp{},
	},
	byte(','),
}

// Allocate a new vendor proto. instance
func New(env *EnviromentCfg) *NbSiHai {
	log.SetLevel(env.LogLevel)
	dbHelper, err := dbh.New(*env)
	if err != nil {
		log.Fatal(err)
	}

	log.Info(fmt.Sprintf("%v", *env))

	return &NbSiHai{
		NetConfig{ // flags
			Addr:           env.TCPAddr,
			HttpAddr:       env.HTTPAddr,
			Protocol:       "tcp",
			ChanSize:       env.QueueSizePerConn,
			WorkerNum:      env.NumWorkersPerConn,
			ReadTimeoutSec: time.Duration(env.TCPTimeOutSec),
			PacketMaxLen:   180,
			StartSymbol:    '+',
			EndSymbol:      '$',
			LogLevel:       env.LogLevel,
			DBAddr:         env.DBAddr,
		},
		dbHelper, VendorStat{},
	}
}

// packet consumer
func (s *NbSiHai) TcpWorker(packetsChan chan *RawTcpPacket) {
	// all time related calculations can be safely ignored when review
	isFirstTime := true
	for {
		timeLast := time.Now()

		// get a packet from the pipe
		packet := <-packetsChan

		success := false
		// handle channel close event
		if packet == nil {
			log.Debug("nil packet, close connection")
			break
		} else {
			// data packet
			if success = s.handlePacket(packet); !success {
				s.Stat.NumInvalidPackets++
				continue
			}
		}
		// micro sec
		var delta uint64 = uint64((time.Now().UnixNano() - timeLast.UnixNano()) / 1000)
		if isFirstTime {
			isFirstTime = false
		} else {
			if s.Stat.AvgWorkerTimeMicroSec == 0 {
				s.Stat.AvgWorkerTimeMicroSec = delta
			} else {
				s.Stat.AvgWorkerTimeMicroSec = (s.Stat.AvgWorkerTimeMicroSec + delta) / 2
			}
		}
	}
}

// helper function
// try to match
func (s *NbSiHai) IsWholePacket(buff []byte, status *int) (bool, error) {
	return buff[len(buff)-1] == s.TcpConfig.EndSymbol, nil
}

func (s *NbSiHai) GetCfg() *NetConfig {
	return &(s.TcpConfig)
}

func (s *NbSiHai) GetStat() *VendorStat {
	s.Stat.AvgDBTimeMicroSec = s.AvgDBTimeMicroSec
	s.Stat.DBWriteMsgCacheSize = uint64(len(s.DBMsgChan))
	s.Stat.NumDBMsgStored = s.NumDBMsgStored
	return &(s.Stat)
}

func (s *NbSiHai) Close() {
	s.Close()
	close(s.DBMsgChan)
}

func (s *NbSiHai) SetLogLevel(lvl log.Level) {
	log.SetLevel(lvl)
}

// private functions
// construct message from the packet
func (s *NbSiHai) handlePacket(packet *RawTcpPacket) bool {
	if packet.Buff[0] != s.TcpConfig.StartSymbol && packet.Buff[len(packet.Buff)-1] != s.TcpConfig.EndSymbol {
		// invalid packet
		s.Stat.NumInvalidPackets++
		log.Error("Invalid packet. Buff:", string(packet.Buff[:]),
			", From:", (*packet.Conn).RemoteAddr())

		return false
	}

	buff := packet.Buff[1 : len(packet.Buff)-1]
	if len(buff) == 0 {
		log.Debug("invalid packet, ignored")
		return false
	}

	//split multi messages in one packet
	var sep bytes.Buffer
	sep.WriteByte(s.TcpConfig.EndSymbol)
	sep.WriteByte(s.TcpConfig.StartSymbol)
	messages := strings.Split(string(buff), sep.String())

	for _, v := range messages {
		parts := strings.Split(v, string(_MessageConstants.Delimiter))
		s.parseMessage(parts, packet.Conn)
	}

	return true
}

// parse one message in a packet
func (s *NbSiHai) parseMessage(parts []string, conn *net.Conn) interface{} {
	var err error = nil
	var lat, lng float64
	if par := _MessageConstants.Commands[parts[0]]; par != nil {
		switch par.(type) {
		case MessageResp:
			_par := MessageResp{}
			if _par.Parse(parts, conn) {
				// convert WGS to GCJ-02
				// false back
				falseBack := false
				if len(_par.Altitude) == 0 {
					_par.Altitude = []byte("0")
					falseBack = true
				}
				if len(_par.Longitude) == 0 {
					_par.Longitude = []byte("0")
					falseBack = true
				}
				if len(_par.Speed) == 0 {
					_par.Speed = []byte("0")
					falseBack = true
				}
				if len(_par.Azimuth) == 0 {
					_par.Azimuth = []byte("0")
					falseBack = true
				}
				if len(_par.Latitude) == 0 {
					_par.Latitude = []byte("0")
					falseBack = true
				}

				lat, err = strconv.ParseFloat(string(_par.Latitude), 64)
				if err == nil {
					lng, err = strconv.ParseFloat(string(_par.Longitude), 64)
					if err == nil {
						lat, lng = gcj.WGStoBD(lat, lng)
						_par.Latitude = []byte(strconv.FormatFloat(lat, 'f', 6, 64))
						_par.Longitude = []byte(strconv.FormatFloat(lng, 'f', 6, 64))
					}
				}
				if falseBack || err != nil {
					log.Error(err, ", Buff:", parts, ", From:", (*conn).RemoteAddr())
				}

				if err == nil {
					// put the message onto the database pipe, assured!!
					// the for-select-break style is my innovation? :)
					for {
						select {
						case s.DBMsgChan <- &_par:
							goto BREAK_
						default:
							// database pipe overflow, pop the oldest one and insert the new one
							<-s.DBMsgChan
							s.Stat.DBWriteMsgDropped++
						}
					}
				BREAK_:
				}
			}
		default:
			log.Error("unkown message", parts, "From", (*conn).RemoteAddr().String())
		}
	} else {
		log.Error("unkown cmd", parts, "From", (*conn).RemoteAddr().String())
	}

	return nil
}
