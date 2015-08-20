// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package ty905

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

const (
	MSG_HEAD = "\x29\x29"
	MSG_TAIL = "\x0D"

	MSG_CMD_UP_NORM_GEO   = byte(0x80)
	MSG_CMD_UP_TIME_PROTO = byte(0xD6)
	MSG_CMD_UP_ACK        = byte(0x85)

	MSG_CMD_DOWN_REP = byte(0x21)
	MSG_CMD_DOWN_CFG = byte(0x7B)
	MSG_CMD_DOWN_MSG = byte(0x3A)

	GEO_DATA_LEN = 34
	MINIMUM_LEN  = 11
)

type Message struct {
	MsgHead, // 2
	MajorCmd, // 1
	Length, //2
	IP, // 4
	Content, // n
	CheckSum, // 1
	MsgTail []byte //1
}

type TY905 struct {
	rawPacket                      RawUdpPacket
	imei, lat, lon, speed, heading string
	gpsTime                        int64
}

func New(rp RawUdpPacket) dbh.IGPSProto {
	return &TY905{rawPacket: rp}
}

func (s *TY905) IsValid() bool {
	if len(s.rawPacket.Buff) > MINIMUM_LEN && bytes.Equal(s.rawPacket.Buff[:2], []byte(MSG_HEAD)) {
		return true
	}
	log.Debug("invalid message: ", s.rawPacket.Buff)
	return false
}

func (s *TY905) IsWhole() bool {
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
	// s.rawPacket.UdpConn.WriteToUDP(s.rawPacket.Buff, s.rawPacket.Remote)
	s.imei = "SHTY905" + strings.ToUpper(hex.EncodeToString(s.rawPacket.Buff[5:9]))

	if s.rawPacket.Buff[2] == MSG_CMD_UP_NORM_GEO {

	}

	return true
}

func (s *TY905) SaveToDB(dbHelper *dbh.DbHelper) error {
	log.Debug("called save to db")
	dbh.SaveToDB(s.imei, s.lat, s.lon, s.speed, s.heading, s.gpsTime, dbHelper)
	return nil
}

func SimNumberToIP(sim []byte) []byte {
	ip_byte := make([]byte, 6)
	barry := make([]byte, 8)

	if len(sim) == 11 {
		tmp, _ := strconv.ParseInt(string(sim[0]), 10, 8)
		binary.LittleEndian.PutUint64(barry, uint64(tmp))
		ip_byte[0] = barry[0]

		for i := 0; i < 5; i++ {
			tmp, _ := strconv.ParseInt(string(sim[2*i+1:2*i+3]), 10, 16)
			binary.LittleEndian.PutUint64(barry, uint64(tmp))
			ip_byte[i+1] = barry[0]
		}

		ip_byte[1] = (byte)((ip_byte[1] - 30))
		if (ip_byte[1] & 0x08) == 0x08 {
			ip_byte[2] = (byte)(ip_byte[2] + 0x80)
		}
		if (ip_byte[1] & 0x04) == 0x04 {
			ip_byte[3] = (byte)(ip_byte[3] + 0x80)
		}
		if (ip_byte[1] & 0x02) == 0x02 {
			ip_byte[4] = (byte)(ip_byte[4] + 0x80)
		}
		if (ip_byte[1] & 0x01) == 0x01 {
			ip_byte[5] = (byte)(ip_byte[5] + 0x80)
		}

		fmt.Println(hex.EncodeToString(ip_byte[2:6]))
		return ip_byte[2:6]
	} else {
		return nil
	}
}
