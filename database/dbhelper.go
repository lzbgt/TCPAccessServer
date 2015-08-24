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
	New(args ...interface{}) IGPSProto
	IsValid() bool
	IsWhole() int
	HandleMsg() bool
	SaveToDB(*DbHelper) error
}

type TCMD struct {
	Id     string
	Type   string
	Params string
	Status string
}

const (
	CMD_TYPE_REPINTV   = "REPINTV"
	CMD_TYPE_SRVADDR   = "SRVADDR"
	CMD_STATUS_APPLIED = "APPLIED"
	CMD_STATUS_PENDING = "PENDING"
)

var _DB *sql.DB = nil
var _ImeiToId map[string]string = nil
var _IdToImei map[string]string = nil
var _CmdsList map[string]*TCMD = nil
var _DBMsgChan chan IDBMessage = nil

var DB *sql.DB = _DB
var DBMsgChan chan IDBMessage = _DBMsgChan

type DbHelper struct {
	*sql.DB
	DBMsgChan chan IDBMessage
}

// initialized in New()
var LbsUrl string = ""

// args: mcc, mnc, lac, cellid
func GetCellLocation(args ...string) (lat, lon string) {
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
func GetCellLocationBD(args ...string) (lat, lon string) {
	lat, lon = GetCellLocation(args...)
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

func SetCmd(deviceId, cmdType string, cmd *TCMD) {
	_CmdsList[deviceId+":"+cmdType] = cmd
}

func DeleteCmd(deviceId, cmdType string) {
	delete(_CmdsList, deviceId+":"+cmdType)
	log.Debug("deleted cmdfrom ram, deviceId=", deviceId, ", cmdType=", cmdType)
}

func DeleteCmdFromDb(cmd *TCMD) {
	CommitCmdToDb(cmd, "DELETED")
}

func GetCmd(deviceId, cmdType string) *TCMD {
	ret, ok := _CmdsList[deviceId+":"+cmdType]
	if ok {
		//
	} else {
		ret = nil
	}
	log.Debug("got cmd from RAM: ", ret)
	return ret
}

func GetCmdTypes() []string {
	var ret []string = make([]string, 0)
	rows, err := _DB.Query(`select type from commandtypes`)
	if err != nil {
		log.Error(err)
		return ret
	}
	defer rows.Close()

	for rows.Next() {
		var deviceType string
		err := rows.Scan(&deviceType)
		if err != nil {
			log.Error(err)
			break
		}
		ret = append(ret, deviceType)
	}

	log.Debug("cmdTypes:", ret)
	return ret
}

func GetCmds(deviceId string) []*TCMD {
	cmdTypes := GetCmdTypes()
	ret := make([]*TCMD, 0)
	for _, v := range cmdTypes {
		if cmd, ok := _CmdsList[deviceId+":"+v]; ok {
			ret = append(ret, cmd)
		}
		log.Debug("got cmds from RAM: ", ret)
	}
	return ret
}

func GetCmdFromDb(deviceId, cmdType string) *TCMD {
	var (
		id_, deviceId_, cmdType_, params_ string
	)
	status_ := "PENDING"
	//select a.id, b.type, a.params, a.status from commands as a left outer join commandtypes as b on  a.type=b.id
	err := _DB.QueryRow(`select a.id,a.deviceId,b.type,a.params from commands 
	as a left outer join commandtypes as b on a.type=b.id where a.status='PENDING' and a.deviceId=? and b.type=?`, deviceId, cmdType).Scan(&id_, &deviceId_, &cmdType_, &params_)

	if err != nil {
		log.Error("failed query cmd for device: ", deviceId, ", type: ", cmdType, " error:", err)
		return nil
	}
	cmd := &TCMD{id_, cmdType_, params_, status_}
	log.Debug("Got cmd from db: ", cmd)

	return cmd
}

func CommitCmdToDb(cmd *TCMD, status string) {
	_, err := _DB.Query("update commands set status=? where id=?", status, cmd.Id)
	log.Debug("committed cmd to: ", cmd, "; status: ", status)
	if err != nil {
		log.Error("failed to commit cmd:", cmd, "error: ", err)
	}
}

func GetImeiById(id string) (string, error) {
	if imei, ok := _IdToImei[id]; ok {
		return imei, nil
	} else {
		rows, err := _DB.Query("select id, deviceImei from device where id=?", id)
		if err != nil {
			log.Error(err, id)
			return "", err
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
			_IdToImei[id] = deviceImei
			_ImeiToId[deviceImei] = id
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

func GetIdByImei(imei string) (string, error) {
	if id, ok := _ImeiToId[imei]; ok {
		return id, nil
	} else {
		qStr := "select id, deviceImei from device where deviceImei=?"
		rows, err := _DB.Query(qStr, imei)
		if err != nil {
			log.Error(err, imei)
			return "", err
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
			_IdToImei[id] = deviceImei
			_ImeiToId[deviceImei] = id
		} else {
			log.Error(sql.ErrNoRows, qStr, imei)
			return "no such device", sql.ErrNoRows
		}

		return id, rows.Err()
	}
}

//user:password@tcp(127.0.0.1:3306)/hello
func New(env EnviromentCfg) *DbHelper {
	log.SetLevel(env.LogLevel)
	log.SetFormatter(&log.TextFormatter{})
	LbsUrl = env.LbsUrl
	var err error = nil

	if _DB == nil {
		_DB, err = sql.Open("mysql", env.DBAddr)
		if err != nil {
			log.Panic(err)
			return nil
		}

		_IdToImei = make(map[string]string)
		_ImeiToId = make(map[string]string)
		_CmdsList = make(map[string]*TCMD)
		_DBMsgChan = make(chan IDBMessage, env.DBCacheSize)
	}

	helper := &DbHelper{_DB, _DBMsgChan}

	_DB.SetMaxIdleConns(env.DBMaxIdleConns)
	_DB.SetMaxOpenConns(env.DBMaxOpenConns)

	var refreshCmdsList = func() {
		errSqlStr := "select from commands error:"
		rows, err := _DB.Query(`select a.id,a.deviceId,b.type,a.params from commands as a  
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

			_cmd, ok := _CmdsList[deviceId+":"+cmdType]
			if ok {
				// modify the status of the old record in DB
				if _cmd.Status != status && _cmd.Id == id {
					//helper.CommitCmdToDb(_cmd, "APPLIED")
				} else if _cmd.Id != id {
					CommitCmdToDb(_cmd, "OVERWRITE")
				}

				// update RAM
				_cmd.Id = id
				_cmd.Params = params
				_cmd.Status = status
				log.Debug("RAM cmd: ", _cmd)

			} else {
				cmd := &TCMD{id, cmdType, params, status}
				log.Debug("RAM cmd: ", cmd)
				_CmdsList[deviceId+":"+cmdType] = cmd
			}
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
				msg := <-_DBMsgChan
				if msg != nil {
					err := msg.SaveToDB(helper)
					if err != nil {
						log.Error(err)
					} else {
						//_DB.NumDBMsgStored++
					}
				}
			}
		}()
	}

	//
	return helper
}

func SaveToDB(imei, lat, lon, speed, heading string, ts int64, dbhelper *DbHelper) error {
	log.Debug("called DBHELPER.SAVETODB")
	id, err := GetIdByImei(imei)
	if err != nil {
		log.Error(err)
		return err
	}

	sqlStr := `INSERT INTO eventdata(deviceId, timestamp, 
	     latitude, longitude, speed, heading) VALUES(?,?,?,?,?,?)`
	stmt, err := _DB.Prepare(sqlStr)
	if err != nil {
		return err
	}

	defer stmt.Close()
	_, err = stmt.Exec(id, ts, lat, lon, speed, heading)
	if err != nil {
		return err
	}

	if lat == "0" && lon == "0" {
		stmt2, err := _DB.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
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
		stmt2, err := _DB.Prepare(`UPDATE devicelatestdata SET lastAckTime=?, 
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
