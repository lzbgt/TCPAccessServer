// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package nbsihai

import (
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"lbsas/utils"
	"net"
	"reflect"

	log "github.com/Sirupsen/logrus"
)

type MessageResp struct {
	Command, //10
	Version, //6
	UID, //15, XX0000-XX-FFFF
	Name, //10, IMEI
	RID, //1, 0-4
	RType, //1, 0|1
	MoveStat, //1, 0|1|2
	Temperature, //4, xx.x
	BattPecent, //3, 0-100
	GPSAccuracy, //<=2, 0|1-50
	Speed, //<=5, 0.0-999.9km/h
	Azimuth, //<=3, 0-359
	Altitude, //<=8, -xxxxx.x
	Longitude, //<=11, -xxx.xxxxx
	Latitude, //<=10, -xx.xxxxxx
	GPSUTime, //14, YYYYMMDDHHMMSS
	MCC, //4, 0XXX
	MNC, //4, 0XXX
	LAC, //4, XXXX
	CID, //4, XXXX
	R1, //0
	R2,
	R3,
	SendTime, //14
	SeqNum []byte //4, 0000-FFFF
}

func (m *MessageResp) LogContent() {
	val := reflect.ValueOf(m).Elem()
	typ := reflect.TypeOf(m).Elem()
	for i := 0; i < val.NumField(); i++ {
		log.Debug(typ.Field(i).Name, string(val.Field(i).Bytes()))
	}
}

func (m *MessageResp) Validate() error {
	return nil
}

func (m *MessageResp) Parse(parts []string, conn *net.Conn) bool {
	log.Debug(reflect.TypeOf(m).String(), "paser called")
	val := reflect.ValueOf(m).Elem()
	if len(parts) != val.NumField() {
		log.Error(ErrorMessage["INVALID_PACKET_LEN"], ", From ", (*conn).RemoteAddr())
		return false
	}
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		valueField.SetBytes([]byte(parts[i]))
	}

	// remove me
	if log.GetLevel() == log.DebugLevel {
		m.LogContent()
	}

	{
		err := m.Validate()
		if err != nil {
			log.Error("ERROR", err, ", Buff:", parts, ", From:", (*conn).RemoteAddr())
			return false
		}
	}

	return true
}

func (s *MessageResp) SaveToDB(dbhelper *dbh.DbHelper) error {

	id, err := dbhelper.GetIdByImei(string(s.UID))
	if err != nil {
		return err
	}

	tm := utils.GetTimestampFromString(s.GPSUTime).UnixNano() / 1000000

	sqlStr := `INSERT INTO eventdata(deviceId, timestamp, 
	     latitude, longitude, speed, heading) VALUES(?,?,?,?,?,?)`
	stmt, err := dbhelper.Prepare(sqlStr)
	if err != nil {
		return err
	}

	defer stmt.Close()

	_, err = stmt.Exec(id, tm, s.Latitude, s.Longitude, s.Speed, s.Azimuth)
	if err != nil {
		return err
	}

	stmt2, err := dbhelper.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
	    latitude=?, longitude=?, speed=?, heading=?, gpsTimestamp=?, updateTime=? where deviceId=?`)
	if err != nil {
		return err
	}

	defer stmt2.Close()

	_, err = stmt2.Exec(tm, s.Latitude, s.Longitude, s.Speed, s.Azimuth, tm, tm, id)
	if err != nil {
		return err
	}

	return nil
}
