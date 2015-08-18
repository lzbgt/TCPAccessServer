// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package database

import (
	"database/sql"
	"encoding/json"
	"io/ioutil"
	. "lbsas/datatypes"
	"lbsas/gcj02"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type IDBMessage interface {
	SaveToDB(*DbHelper) error
}

type IGPSProto interface {
	New(interface{}) IGPSProto
	IsValid() bool
	HandleMsg() bool
	SaveToDB(*DbHelper) error
}

type TCMD struct {
	Id     string
	Type   string
	Params string
	Status string
}

type DbHelper struct {
	*sql.DB
	ImeiToId  map[string]string
	IdToImei  map[string]string
	CmdsList  map[string]*TCMD
	DBMsgChan chan IDBMessage
	// stat
	AvgDBTimeMicroSec, NumDBMsgStored uint64
}

// initialized in New()
var LbsUrl string = ""

// args: mcc, mnc, lac, cellid
func (s *DbHelper) GetCellLocation(args ...string) (lat, lon string) {
	lat, lon = "0", "0"
	if len(args) < 4 {
		return
	}
	resp, err := http.PostForm(LbsUrl,
		url.Values{"mcc": {args[0]}, "mnc": {args[1]},
			"lac": {args[2]}, "cell": {args[3]}})
	if resp != nil && err == nil {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			var loc WSGLocation
			err := json.Unmarshal(body, &loc)
			if err == nil {
				lat = loc.Lat
				lon = loc.Lon
			}
		}
	}

	return
}

// get baidu position
func (s *DbHelper) GetCellLocationBD(args ...string) (lat, lon string) {
	lat, lon = s.GetCellLocation(args...)
	if lat == "0" && lon == "0" {
		log.Error("can't find LOC for LBS:", strings.Join(args, ","))
		return
	}

	latDouble, err := strconv.ParseFloat(lat, 64)
	if err == nil {
		lonDouble, err := strconv.ParseFloat(lon, 64)
		if err == nil {
			latDouble, lonDouble = gcj02.WGStoBD(latDouble, lonDouble)
			lat = strconv.FormatFloat(latDouble, 'f', 6, 64)
			lon = strconv.FormatFloat(lonDouble, 'f', 6, 64)
		}
	}
	if err != nil {
		log.Error("eorr GetCellLocationBD: ", err)
	}
	return
}

func (s *DbHelper) SetCmd(deviceId, cmdType string, cmd *TCMD) {
	s.CmdsList[deviceId+":"+cmdType] = cmd
}

func (s *DbHelper) DeleteCmd(deviceId, cmdType string) {
	delete(s.CmdsList, deviceId+":"+cmdType)
	log.Debug("deleted cmdfrom ram, deviceId=", deviceId, ", cmdType=", cmdType)
}

func (s *DbHelper) DeleteCmdFromDb(cmd *TCMD) {
	s.CommitCmdToDb(cmd, "DELETED")
}

func (s *DbHelper) GetCmd(deviceId, cmdType string) *TCMD {
	ret, ok := s.CmdsList[deviceId+":"+cmdType]
	if ok {
		//
	} else {
		ret = nil
	}
	log.Debug("got cmd from RAM: ", ret)
	return ret
}

func (s *DbHelper) GetCmdFromDb(deviceId, cmdType string) *TCMD {
	var (
		id_, deviceId_, cmdType_, params_ string
	)
	status_ := "PENDING"
	//select a.id, b.type, a.params, a.status from commands as a left outer join commandtypes as b on  a.type=b.id
	err := s.QueryRow(`select a.id,a.deviceId,b.type,a.params from commands 
	as a left outer join commandtypes as b on a.type=b.id where a.status='PENDING' and a.deviceId=? and b.type=?`, deviceId, cmdType).Scan(&id_, &deviceId_, &cmdType_, &params_)

	if err != nil {
		log.Error("failed query cmd for device: ", deviceId, ", type: ", cmdType, " error:", err)
		return nil
	}
	cmd := &TCMD{id_, cmdType_, params_, status_}
	log.Debug("Got cmd from db: ", cmd)

	return cmd
}

func (s *DbHelper) CommitCmdToDb(cmd *TCMD, status string) {
	_, err := s.Query("update commands set status=? where id=?", status, cmd.Id)
	log.Debug("committed cmd to: ", cmd, "; status: ", status)
	if err != nil {
		log.Error("failed to commit cmd:", cmd, "error: ", err)
	}
}

func (s *DbHelper) GetImeiById(id string) (string, error) {
	if imei, ok := s.IdToImei[id]; ok {
		return imei, nil
	} else {
		rows, err := s.Query("select id, deviceImei from device where id=?", id)
		if err != nil {
			log.Error(err, id)
		}

		defer rows.Close()
		var (
			id         string
			deviceImei string
		)
		if rows.Next() {
			err := rows.Scan(&id, &deviceImei)
			if err != nil {
				log.Error(err)
			}
			s.IdToImei[id] = deviceImei
			s.ImeiToId[deviceImei] = id
		} else {
			return "no such device", sql.ErrNoRows
		}
		err = rows.Err()
		if err != nil {
			log.Error(err)
		}
		return deviceImei, nil
	}
}

func (s *DbHelper) GetIdByImei(imei string) (string, error) {
	if id, ok := s.ImeiToId[imei]; ok {
		return id, nil
	} else {
		qStr := "select id, deviceImei from device where deviceImei=?"
		rows, err := s.Query(qStr, imei)
		if err != nil {
			log.Error(err, imei)
		}

		defer rows.Close()
		var (
			id         string
			deviceImei string
		)
		if rows.Next() {
			err := rows.Scan(&id, &deviceImei)
			if err != nil {
				log.Error(err, ", Query:", qStr, imei)
			}
			s.IdToImei[id] = deviceImei
			s.ImeiToId[deviceImei] = id
		} else {
			log.Error(sql.ErrNoRows, qStr, imei)
			return "no such device", sql.ErrNoRows
		}

		return id, rows.Err()
	}
}

//user:password@tcp(127.0.0.1:3306)/hello
func New(env EnviromentCfg) (*DbHelper, error) {
	log.SetLevel(env.LogLevel)
	log.SetFormatter(&log.TextFormatter{})
	LbsUrl = env.LbsUrl

	db_, err := sql.Open("mysql", env.DBAddr)
	if err != nil {
		log.Panic(err)
	}

	helper := &DbHelper{db_, make(map[string]string), make(map[string]string),
		make(map[string]*TCMD), make(chan IDBMessage, env.DBCacheSize), 0, 0}

	if err != nil {
		return nil, err
	}

	// restrain number of connections
	helper.SetMaxIdleConns(env.DBMaxIdleConns)
	helper.SetMaxOpenConns(env.DBMaxOpenConns)

	var refreshCmdsList = func() {
		errSqlStr := "select from commands error:"
		rows, err := helper.Query(`select a.id,a.deviceId,b.type,a.params from commands as a  
		left outer join commandtypes as b on a.type=b.id where status='PENDING'`)
		if err != nil {
			log.Error(errSqlStr, err)
			return
		}

		defer rows.Close()
		var (
			id, deviceId, cmdType, params, status string
		)

		status = "PENDING"

		for rows.Next() {
			err := rows.Scan(&id, &deviceId, &cmdType, &params)
			if err != nil {
				log.Error(err)
				break
			}
			//imei, ok := helper.IdToImei[id]
			//if ok {
			//add to map
			_cmd, ok := helper.CmdsList[deviceId+":"+cmdType]
			if ok {
				// modify the status of the old record in DB
				if _cmd.Status != status && _cmd.Id == id {
					//helper.CommitCmdToDb(_cmd, "APPLIED")
				} else if _cmd.Id != id {
					helper.CommitCmdToDb(_cmd, "OVERWRITE")
				}

				// update RAM
				_cmd.Id = id
				_cmd.Params = params
				_cmd.Status = status
				log.Debug("RAM cmd: ", _cmd)

			} else {
				cmd := &TCMD{id, cmdType, params, status}
				log.Debug("RAM cmd: ", cmd)
				helper.CmdsList[deviceId+":"+cmdType] = cmd
			}
			//}

		}
		err = rows.Err()
		if err != nil {
			log.Error(errSqlStr, err)
		}
	}

	// periodically update commands list
	go func() {
		timeChan := time.NewTicker(time.Second * 60 / 2).C
		for {
			<-timeChan
			//
			refreshCmdsList()
		}
	}()

	// setup dbworker pool
	for i := 0; i < env.DBMaxOpenConns; i++ {
		go func() {
			for {
				msg := <-helper.DBMsgChan
				// ignore chan-close message
				if msg != nil {
					timeLast := time.Now()
					// process db msg
					err := msg.SaveToDB(helper)
					if err != nil {
						log.Error(err)
					} else {
						helper.NumDBMsgStored++
					}

					// messure the time in micro sec
					delta := uint64((time.Now().UnixNano() - timeLast.UnixNano()) / 1000)
					if helper.AvgDBTimeMicroSec == 0 {
						helper.AvgDBTimeMicroSec = delta
					} else {
						helper.AvgDBTimeMicroSec = (helper.AvgDBTimeMicroSec + delta) / 2
					}
				}
			}
		}()
	}

	//
	return helper, err
}

func SaveToDB(imei, lat, lon, speed, heading string, ts int64, dbhelper *DbHelper) error {
	log.Debug("called DBHELPER.SAVETODB")
	id, err := dbhelper.GetIdByImei(imei)
	if err != nil {
		log.Error(err)
		return err
	}

	sqlStr := `INSERT INTO eventdata(deviceId, timestamp, 
	     latitude, longitude, speed, heading) VALUES(?,?,?,?,?,?)`
	stmt, err := dbhelper.Prepare(sqlStr)
	if err != nil {
		return err
	}

	defer stmt.Close()
	_, err = stmt.Exec(id, ts, lat, lon, speed, heading)
	if err != nil {
		return err
	}

	if lat == "0" && lon == "0" {
		stmt2, err := dbhelper.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
		speed=?, heading=?, gpsTimestamp=?, updateTime=? where deviceId=?`)
		if err != nil {
			return err
		}
		defer stmt2.Close()

		_, err = stmt2.Exec(ts, speed, heading, ts, ts, id)
		if err != nil {
			return err
		}

	} else {
		stmt2, err := dbhelper.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
	    latitude=?, longitude=?, speed=?, heading=?, gpsTimestamp=?, updateTime=? where deviceId=?`)
		if err != nil {
			return err
		}
		defer stmt2.Close()

		_, err = stmt2.Exec(ts, lat, lon, speed, heading, ts, ts, id)
		if err != nil {
			return err
		}

	}
	return nil
}
