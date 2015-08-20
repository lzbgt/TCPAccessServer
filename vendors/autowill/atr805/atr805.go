package atr805

import (
	"bytes"
	"encoding/hex"
	"fmt"
	dbh "lbsas/database"
	"lbsas/gcj02"
	"lbsas/tcp2"
	"lbsas/utils"
	"strconv"
	"strings"

	log "github.com/Sirupsen/logrus"
)

const (
	PROTO_IDENTIFIER = "\x92\x29"

	PACKET_UP_GPS    = byte(0x80)
	PACKET_UP_LBS    = byte(0x86)
	PACKET_DOWN_REP  = byte(0x21)
	PACKET_DOWN_MODE = byte(0x7f)
	PACKET_DOWN_ADDR = byte(0x79)

	MINIMUM_LEN = 15
)

type Atr805 struct {
	buff                           []byte
	imei, lat, lon, speed, heading string
	gpsTime                        int64
}

func New(buff []byte) dbh.IGPSProto {
	return &Atr805{buff: buff}
}

func (s *Atr805) New(param interface{}) dbh.IGPSProto {
	if buff, ok := param.([]byte); ok {
		log.Debug("received0 : ", hex.EncodeToString(buff))
		return &Atr805{buff: buff}
	}
	return nil
}

func (s *Atr805) IsValid() bool {
	log.Debug("received: ", hex.EncodeToString(s.buff))
	if len(s.buff) >= MINIMUM_LEN && bytes.Equal(s.buff[:2], []byte(PROTO_IDENTIFIER)) {
		return true
	}
	log.Debug("proto unsatisfied: ", s.buff)
	return false
}

func (s *Atr805) IsWhole() bool {
	b := int(s.buff[3]<<8 + s.buff[4])
	if len(s.buff[5:]) == b && s.buff[len(s.buff)-1] == '\x0d' {
		return true
	}
	log.Error("invalid length:", s.buff)
	return true
}

// true to store in DB, false otherwise
func (s *Atr805) HandleMsg() bool {
	log.Debug("handlemsg called")
	// s.rawPacket.UdpConn.WriteToUDP(s.rawPacket.Buff, s.rawPacket.Remote)
	s.imei = "ATR" + strings.ToUpper(hex.EncodeToString(s.buff[5:11]))

	if s.buff[2] == PACKET_UP_GPS {
		lat := float64(utils.DecodeTY905Byte(s.buff[0xb])) + (float64(utils.DecodeTY905Byte(s.buff[0xc]))+
			float64(utils.DecodeTY905Byte(s.buff[0xd]))/100+float64(utils.DecodeTY905Byte(s.buff[0xe]))/10000)/90
		lon := float64(utils.DecodeTY905Byte(s.buff[0x12])) + float64(utils.DecodeTY905Byte(s.buff[0x13]))/100 + float64(utils.DecodeTY905Byte(s.buff[0x14]))/10000
		lon = lon/90 + float64(utils.DecodeTY905Byte(s.buff[0x10]))*100 + float64(utils.DecodeTY905Byte(s.buff[0x11]))
		lat, lon = gcj02.WGStoBD(lat, lon)
		s.lat = strconv.FormatFloat(lat, 'f', 4, 64)
		s.lon = strconv.FormatFloat(lon, 'f', 4, 64)
		log.Debug("lat:", lat, " lon:", lon)
		d := fmt.Sprintf("%02d%02d%02d", utils.DecodeTY905Byte(s.buff[0x16]), utils.DecodeTY905Byte(s.buff[0x17]), utils.DecodeTY905Byte(s.buff[0x18]))
		d = d + fmt.Sprintf("%02d%02d%02d", utils.DecodeTY905Byte(s.buff[0x19]), utils.DecodeTY905Byte(s.buff[0x1a]), utils.DecodeTY905Byte(s.buff[0x1b]))
		s.gpsTime = utils.GetTimestampFromString([]byte(d)).UnixNano() / 1000000
		s.heading = "0"
		s.speed = "0"
		return true
	} else if s.buff[2] == PACKET_UP_LBS {
		//numCells := s.buff[0xb]
		//base := int(0x0c)

	}

	return true
}

func (s *Atr805) SaveToDB(dbHelper *dbh.DbHelper) error {
	log.Debug("called save to db")
	dbh.SaveToDB(s.imei, s.lat, s.lon, s.speed, s.heading, s.gpsTime, dbHelper)
	return nil
}

func init() {
	log.SetLevel(log.DebugLevel)
	tcp2.Register(New(nil))
	log.Debug("registered")
}
