package atr805

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	dbh "lbsas/database"
	"lbsas/gcj02"
	"lbsas/tcp2"
	"lbsas/utils"
	"net"
	"strconv"
	"strings"
	"time"

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
	buff []byte
	imei, lat, lon,
	speed, heading string
	gpsTime int64
	conn    *net.Conn
}

type LBSData struct {
	MCC, MNC, LAC, CellID string
	Power, TA             byte
}

func New() dbh.IGPSProto {
	return &Atr805{}
}

func (s *Atr805) New(args ...interface{}) dbh.IGPSProto {
	if len(args) == 2 {
		if buff, ok := args[0].([]byte); ok {
			log.Debug("received0 : ", hex.EncodeToString(buff))
			if conn, ok := args[1].(*net.Conn); ok {
				return &Atr805{buff: buff, conn: conn}
			}
		}
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

func (s *Atr805) IsWhole() int {
	// multi-partial packets enhencement.
	b := int(s.buff[3])<<8 + int(s.buff[4])
	if len(s.buff[5:]) == b && s.buff[len(s.buff)-1] == '\x0d' {
		return 0
	} else if len(s.buff[5:]) > b && s.buff[b+5] == '\x0d' {
		return len(s.buff) - b - 5
	}

	log.Error("invalid length:", s.buff, "expected:", b, "actual:", len(s.buff[5:]), "last:", s.buff[len(s.buff)-1])
	return -1
}

//
func (s *Atr805) HandleMsg() bool {
	log.Debug("handlemsg called")
	// s.rawPacket.UdpConn.WriteToUDP(s.rawPacket.Buff, s.rawPacket.Remote)
	s.imei = "ATR" + strings.ToUpper(hex.EncodeToString(s.buff[5:11]))

	handleCmds(s)

	if s.buff[2] == PACKET_UP_GPS {
		lat := float64(utils.DecodeTY905Byte(s.buff[0xb])) + (float64(utils.DecodeTY905Byte(s.buff[0xc]))+
			float64(utils.DecodeTY905Byte(s.buff[0xd]))/100+float64(utils.DecodeTY905Byte(s.buff[0xe]))/10000)/60
		lon := float64(utils.DecodeTY905Byte(s.buff[0x12])) + float64(utils.DecodeTY905Byte(s.buff[0x13]))/100 + float64(utils.DecodeTY905Byte(s.buff[0x14]))/10000
		lon = lon/60 + float64(utils.DecodeTY905Byte(s.buff[0x10]))*100 + float64(utils.DecodeTY905Byte(s.buff[0x11]))
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
		numcells := int(s.buff[0xb])
		lbsdata := make([]LBSData, numcells)
		width := 11
		base := int(0x0c)
		var lat, lon string
		var i int
		for i = 0; i < numcells; i++ {
			index := base + i*width
			mcc := strconv.Itoa(int(utils.DecodeTY905Byte(s.buff[index]))*100 + int(utils.DecodeTY905Byte(s.buff[index+1])))
			mnc := strconv.Itoa(int(utils.DecodeTY905Byte(s.buff[index+2])))
			lac := strconv.Itoa(int(utils.DecodeTY905Byte(s.buff[index+3]))*10000 + int(utils.DecodeTY905Byte(s.buff[index+4]))*100 +
				int(utils.DecodeTY905Byte(s.buff[index+5])))
			cid := strconv.Itoa(int(utils.DecodeTY905Byte(s.buff[index+6]))*10000 + int(utils.DecodeTY905Byte(s.buff[index+7]))*100 + int(utils.DecodeTY905Byte(s.buff[index+8])))
			pwr := utils.DecodeTY905Byte(s.buff[index+9])
			ta := utils.DecodeTY905Byte(s.buff[index+10])
			lbsdata[i] = LBSData{mcc, mnc, lac, cid, pwr, ta}
			// TODO: we will improve the accuracy in the future,
			// but for now we just simply look for the first one available
			lat, lon = dbh.GetCellLocationBD(mcc, mnc, lac, cid)
			if lat != "0" && lac != "0" {
				break
			}
		}
		if i != numcells {
			s.lat = lat
			s.lon = lon
			s.gpsTime = time.Now().UnixNano() / 1000000
			return true
		}
	}

	return false
}

func (s *Atr805) SaveToDB(dbHelper *dbh.DbHelper) error {
	log.Debug("called save to db")
	dbh.SaveToDB(s.imei, s.lat, s.lon, s.speed, s.heading, s.gpsTime, dbHelper)
	return nil
}

// --- cmd related code
type TCmdFunc func(*dbh.TCMD, *Atr805) bool

var _cmdMap = map[string]TCmdFunc{
	dbh.CMD_TYPE_REPINTV: handleCmdRepInterval,
}

//
func confirmMessage(atr *Atr805) bool {
	head := []byte("\x92\x29\x21\x00\x0a")
	sn := utils.EncodeCBCDFromString(atr.imei[3:])
	rest := []byte{atr.buff[2], 0xFF, 0xFF, 0x0D}
	cmdBuff := bytes.Join([][]byte{head, sn, rest}, nil)
	log.Debug("confirm msg: ", hex.EncodeToString(cmdBuff))
	(*atr.conn).Write(cmdBuff)
	return true
}

//
func handleCmdRepInterval(cmd *dbh.TCMD, atr *Atr805) bool {
	params := strings.Split(cmd.Params, ",")
	if len(params) == 2 && len(params[0]) == 4 && len(params[1]) > 0 {
		head := []byte("\x92\x29\x7F\x00\x1D")
		sn := utils.EncodeCBCDFromString(atr.imei[3:])
		mask_retry := []byte("\x01\x0A")
		interval := make([]byte, 4)
		_interval, err := strconv.Atoi(params[1])
		if err != nil {
			log.Error(err, cmd)
			dbh.CommitCmdToDb(cmd, "INVALID")
			return false
		}
		binary.BigEndian.PutUint32(interval, uint32(_interval))
		tail := []byte("\xff\x0d")
		cmdBuff := bytes.Join([][]byte{head, sn, mask_retry, interval, interval, tail}, nil)
		log.Info("applied cmd: ", hex.EncodeToString(cmdBuff))
		(*atr.conn).Write(cmdBuff)
		cmd.Status = dbh.CMD_STATUS_APPLIED
		dbh.CommitCmdToDb(cmd, dbh.CMD_STATUS_APPLIED)
		return true
	}
	return false
}

//
func handleCmds(atr *Atr805) bool {
	//
	imei := atr.imei
	id, err := dbh.GetIdByImei(imei)
	if err != nil {
		log.Error("device not existed: ", imei, err)
		return false
	}

	cmds := dbh.GetCmds(id)
	for _, v := range cmds {
		if v == nil {
			continue
		}
		if v.Status == dbh.CMD_STATUS_PENDING {
			log.Debug("got cmd: ", v)
			_cmd := dbh.GetCmdFromDb(id, v.Type)
			if _cmd != nil {
				if v.Id != _cmd.Id {
					dbh.CommitCmdToDb(v, "OVERWRITE")
				}
				v.Params = _cmd.Params
				v.Id = _cmd.Id
			} else {
				dbh.DeleteCmd(id, v.Type)
				goto HANDLED_CMD
			}

			if fn, ok := _cmdMap[_cmd.Type]; ok {
				fn(_cmd, atr)
			}
		}
	}

HANDLED_CMD:
	// TODO need device to test
	confirmMessage(atr)
	return true
}

func init() {
	log.SetLevel(log.DebugLevel)
	tcp2.Register(New())
	log.Debug("registered")
}
