// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-07-14	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package eworld

import (
	"bytes"
	dbh "lbsas/database"
	. "lbsas/datatypes"
	"lbsas/utils"
	"net"
	"reflect"
	"time"

	log "github.com/Sirupsen/logrus"
)

// RESP message
type GenRespMsg struct {
	Vendor,
	SN,
	Version, // 10
	Time, // HHMMSS
	Valid, // A/V
	Latitude, //
	NS, // N/S
	Longitude, //
	EW, // E/W
	Speed, //
	Azimuth, //
	Date, // DDMMYY
	Status,
	Power []byte
}

func (m *GenRespMsg) LogContent() {
	val := reflect.ValueOf(m).Elem()
	typ := reflect.TypeOf(m).Elem()
	for i := 0; i < val.NumField(); i++ {
		log.Info(typ.Field(i).Name, ": ", string(val.Field(i).Bytes()))
	}
}

func (m *GenRespMsg) Validate() error {
	return nil
}

func (m *GenRespMsg) Parse(parts []string, conn *net.Conn) bool {
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

func (s *GenRespMsg) SaveToDB(dbhelper *dbh.DbHelper) error {

	id, err := dbhelper.GetIdByImei("WORLD" + string(s.SN))
	if err != nil {
		return err
	}

	// get the time
	mTime := bytes.Join([][]byte{s.Date[4:6], s.Date[2:4], s.Date[0:2], s.Time}, nil)
	tm := utils.GetTimestampFromString(mTime).UnixNano() / 1000000
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

//
// ACK message
type ACKMsgGTGBC struct {
	Command, //10
	Version, //6
	UID, //15, XX0000-XX-FFFF
	Name, //10, IMEI
	SeqNum, //4, 0000-FFFF
	SendTime, //14
	CntNum []byte //4, 0000-FFFF
}

//
// LSV RESP message
type LbsRespMsg struct {
	Vendor,
	SN,
	LBS,
	MCC,
	MNC,
	LAC,
	CELL,
	Unknown,
	Status,
	Power []byte
}

func (m *LbsRespMsg) Parse(parts []string, conn *net.Conn) bool {
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

func (s *LbsRespMsg) SaveToDB(dbhelper *dbh.DbHelper) error {

	id, err := dbhelper.GetIdByImei("WORLD" + string(s.SN))
	if err != nil {
		return err
	}

	lat, lon := dbhelper.GetCellLocationBD(string(s.MCC), string(s.MNC), string(s.LAC), string(s.CELL))
	// get the time
	log.Debug("LBS lat:", lat, ",lon:", lon)
	tm := time.Now().UnixNano() / 1000000
	sqlStr := `INSERT INTO eventdata(deviceId, timestamp, 
	     latitude, longitude, speed, heading) VALUES(?,?,?,?,?,?)`
	stmt, err := dbhelper.Prepare(sqlStr)
	if err != nil {
		return err
	}

	defer stmt.Close()
	_, err = stmt.Exec(id, tm, lat, lon, 0, 0)
	if err != nil {
		return err
	}

	if lat == 0 && lon == 0 {
		return nil
	}

	stmt2, err := dbhelper.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
	    latitude=?, longitude=?, speed=?, heading=?, gpsTimestamp=?, updateTime=? where deviceId=?`)
	if err != nil {
		return err
	}
	defer stmt2.Close()

	_, err = stmt2.Exec(tm, lat, lon, 0, 0, tm, tm, id)
	if err != nil {
		return err
	}

	return nil
}

func (m *LbsRespMsg) LogContent() {
	val := reflect.ValueOf(m).Elem()
	typ := reflect.TypeOf(m).Elem()
	for i := 0; i < val.NumField(); i++ {
		log.Info(typ.Field(i).Name, ": ", string(val.Field(i).Bytes()))
	}
}

func (m *LbsRespMsg) Validate() error {
	return nil
}
