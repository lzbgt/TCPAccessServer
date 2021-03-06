// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-07-14	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package eworld

import (
	"bytes"
	"fmt"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"lbsas/gcj02"
	"net"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

const (
	CMD_STATUS_OVERWRITE = "OVERWRITE"
	CMD_STATUS_APPLIED   = "APPLIED"
	CMD_STATUS_PENDING   = "PENDING"
	CMD_TYPE_REPINTV     = "REPINTV"
)

// module exported global variables
type EWorld struct {
	TcpConfig NetConfig
	*dbh.DbHelper
	Stat VendorStat
}

// module private variables
var _MessageConstants = &struct {
	Commands  map[string]interface{}
	Delimiter byte
}{
	map[string]interface{}{
		"V": GenRespMsg{},
		"L": LbsRespMsg{},
	},
	byte(','),
}

// Allocate a new vendor proto. instance
func New(env *EnviromentCfg) *EWorld {
	log.SetLevel(env.LogLevel)
	dbHelper := dbh.New(*env)

	//
	if dbHelper == nil {
		panic("Cann't connect to database")
	}

	log.Info(fmt.Sprintf("%v", *env))

	return &EWorld{
		NetConfig{ // flags
			Addr:           env.TCPAddr,
			HttpAddr:       env.HTTPAddr,
			Protocol:       "tcp",
			ChanSize:       env.QueueSizePerConn,
			WorkerNum:      env.NumWorkersPerConn,
			ReadTimeoutSec: time.Duration(env.TCPTimeOutSec),
			PacketMaxLen:   180,
			StartSymbol:    '*',
			EndSymbol:      '#',
			LogLevel:       env.LogLevel,
			DBAddr:         env.DBAddr,
		},
		dbHelper, VendorStat{},
	}
}

// packet consumer
func (s *EWorld) TcpWorker(packetsChan chan *RawTcpPacket) {
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
func (s *EWorld) IsWholePacket(buff []byte, status *int) (bool, error) {
	for i := 0; i < len(buff); i++ {
		switch buff[i] {
		case s.TcpConfig.EndSymbol:
			(*status)++
		}
	}

	ret := (*status%2 == 0) && (buff[len(buff)-1] == s.TcpConfig.EndSymbol)
	// log.Debug("status:", *status, "ret:", ret, buff[len(buff)-2], buff[len(buff)-1], s.TcpConfig.EndSymbol)
	return ret, nil

}

func (s *EWorld) GetCfg() *NetConfig {
	return &(s.TcpConfig)
}

func (s *EWorld) Close() {
	s.Close()
	close(s.DBMsgChan)
}

func (s *EWorld) SetLogLevel(lvl log.Level) {
	log.SetLevel(lvl)
}

// private functions
// construct message from the packet
func (s *EWorld) handlePacket(packet *RawTcpPacket) bool {
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

// reply messages
func (s *EWorld) handleCmds(sn string, conn *net.Conn) bool {
	//
	imei := "WORLD" + sn
	id, err := dbh.GetIdByImei(imei)
	if err != nil {
		log.Error("device not existed: ", imei, err)
		return false
	}

	sentCmd := false
	cmdError := false
	// *TH,2020916012,I1,050400,0,0,14,XRDDCS12001440#
	cmd := dbh.GetCmd(id, CMD_TYPE_REPINTV)
	//log.Debug("query for cmd: ", id, ":", CMD_TYPE_REPINTV)
	if cmd != nil && cmd.Status == CMD_STATUS_PENDING {
		log.Debug("got cmd: ", cmd)
		_cmd := dbh.GetCmdFromDb(id, CMD_TYPE_REPINTV)
		if _cmd != nil {
			if cmd.Id != _cmd.Id {
				dbh.CommitCmdToDb(cmd, "OVERWRITE")
			}

			cmd.Params = _cmd.Params
			cmd.Id = _cmd.Id
		} else {
			dbh.DeleteCmd(id, CMD_TYPE_REPINTV)
			goto HANDLED_CMD
		}

		params := strings.Split(cmd.Params, ",")
		if len(params) == 2 && len(params[0]) == 4 && len(params[1]) > 0 {
			h, err := strconv.ParseInt(params[0][0:2], 10, 16)
			if err != nil || h < 0 || h > 24 {
				cmdError = true
				goto HANDLED_CMD
			}
			// UTC to UTC+8
			h = (h + 8) % 24
			m, err := strconv.ParseInt(params[0][2:4], 10, 16)
			if err != nil || m < 0 || m > 59 {
				cmdError = true
				goto HANDLED_CMD
			}

			interval, err := strconv.ParseInt(params[1], 10, 16)
			if err != nil || interval < 0 || interval > 1440 {
				cmdError = true
				goto HANDLED_CMD
			}
			cfg := fmt.Sprintf("%02d%02d", h, m) + fmt.Sprintf("%04d", interval)
			ackFormat := "*TH,%s,I2,050400,0,0,14,XRDDCS%s#"
			theCmd := []byte(fmt.Sprintf(ackFormat, imei[5:], cfg))
			log.Info("applied cmd: ", theCmd)
			(*conn).Write(theCmd)
			sentCmd = true
			cmd.Status = CMD_STATUS_APPLIED
			log.Debug("cmd sent: ", cmd)
			dbh.CommitCmdToDb(cmd, CMD_STATUS_APPLIED)
		}
	}

HANDLED_CMD:
	if cmdError {
		log.Error("invalid cmd:", cmd)
		dbh.CommitCmdToDb(cmd, "INVALID")
	}

	if !sentCmd {
		// reply the message
		// *TH,2020916012,I1,050400,0,0,6,XRDDCP#
		ackFormat := "*TH,%s,I1,%s,0,0,6,XRDDCP#"
		tm := time.Now()
		hhmmss := fmt.Sprintf("%02d%02d%02d", tm.Hour(), tm.Minute(), tm.Second())
		(*conn).Write([]byte(fmt.Sprintf(ackFormat, imei[5:], hhmmss)))
	}

	return true
}

// parse one message in a packet
func (s *EWorld) parseMessage(parts []string, conn *net.Conn) interface{} {
	var err error = nil
	var lat, lng float64
	if len(parts) < 3 || len(parts[2]) < 1 || len(parts[1]) < 1 {
		return nil
	}

	if par := _MessageConstants.Commands[parts[2][0:1]]; par != nil {
		var dbmsg dbh.IDBMessage

		switch par.(type) {
		case GenRespMsg:
			_par := GenRespMsg{}
			if _par.Parse(parts, conn) {
				s.handleCmds(parts[1], conn)
				// convert WGS to GCJ-02
				if len(_par.Latitude) == 0 {
					_par.Latitude = []byte("0")
				}
				if len(_par.Longitude) == 0 {
					_par.Longitude = []byte("0")
				}
				if len(_par.Speed) == 0 {
					_par.Speed = []byte("0")
				}
				if len(_par.Azimuth) == 0 {
					_par.Azimuth = []byte("0")
				}

				var (
					f1, f2 float64
				)

				// DDFF.FFFF
				if len(_par.Latitude) == 9 {
					f1, err = strconv.ParseFloat(string(_par.Latitude[0:2]), 64)
					if err == nil {
						f2, err = strconv.ParseFloat(string(_par.Latitude[2:]), 64)
					}

					if err != nil {
						log.Error("error in convert Latitude: ", _par)
						return nil
					}
					lat = f1 + f2/60
				} else {
					log.Error("invalid latitude, use 0 instead: ", _par)
					lat = 0
				}
				// DDDFF.FFFF
				if len(_par.Longitude) == 10 {
					f1, err = strconv.ParseFloat(string(_par.Longitude[0:3]), 64)
					if err == nil {
						f2, err = strconv.ParseFloat(string(_par.Longitude[3:]), 64)
					}
					if err != nil {
						log.Error("error in convert Longtitude: ", _par)
						return nil
					}
					lng = f1 + f2/60
				} else {
					log.Error("invalid longitude, use 0 instead: ", _par)
					lng = 0
				}
				lat, lng = gcj02.WGStoBD(lat, lng)
				_par.Latitude = []byte(strconv.FormatFloat(lat, 'f', 6, 64))
				_par.Longitude = []byte(strconv.FormatFloat(lng, 'f', 6, 64))
			}
			dbmsg = &_par
		case LbsRespMsg:
			_par := LbsRespMsg{}
			if _par.Parse(parts, conn) {
				s.handleCmds(parts[1], conn)
			}
			dbmsg = &_par

		default:
			log.Error("unkown message", parts, "From", (*conn).RemoteAddr().String())
		}

		if err == nil {
			// put the message onto the database pipe, assured!!
			// the for-select-break style is my innovation? :)
			for {
				select {
				case s.DBMsgChan <- dbmsg:
					goto BREAK_
				default:
					// database pipe overflow, pop the oldest one and insert the new one
					<-s.DBMsgChan
					s.Stat.DBWriteMsgDropped++
				}
			}
		BREAK_:
		}
	} else {
		log.Error("unkown cmd", parts, "From", (*conn).RemoteAddr().String())
	}

	return nil
}
