// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package ty905

import (
	dbh "lbsas/database"
	. "lbsas/datatypes"

	log "github.com/Sirupsen/logrus"
)

const (
	MSG_HEAD = "\x29\x29"
	MSG_TAIL = "\x0D"

	MSG_CMD_UP_NORM_GEO   = "\x80"
	MSG_CMD_UP_TIME_PROTO = "\xD6"
	MSG_CMD_UP_ACK        = "\x85"

	MSG_CMD_DOWN_REP = "\x21"
	MSG_CMD_DOWN_CFG = "\x7B"
	MSG_CMD_DOWN_MSG = "\x3A"

	GEO_DATA_LEN = 34
)

type TY905 struct {
	rawPacket                      RawUdpPacket
	imei, lat, lon, speed, heading string
	gpsTime                        int64
}

func New(rp RawUdpPacket) dbh.IGPSProto {
	return &TY905{rawPacket: rp}
}

func (s *TY905) IsValid() bool {
	return true
}

func (s *TY905) New(rp interface{}) dbh.IGPSProto {
	_rp, ok := rp.(RawUdpPacket)
	if ok {
		return &TY905{rawPacket: _rp}
	} else {
		log.Error("NIL TY905")
		return nil
	}
}

// true to store in DB, false otherwise
func (s *TY905) HandleMsg() bool {
	log.Debug("handlemsg called")
	s.rawPacket.UdpConn.WriteToUDP(s.rawPacket.Buff, s.rawPacket.Remote)

	//

	return true
}

func (s *TY905) SaveToDB(dbHelper *dbh.DbHelper) error {
	log.Debug("called save to db")
	dbh.SaveToDB(s.imei, s.lat, s.lon, s.speed, s.heading, s.gpsTime, dbHelper)
	return nil
}
