// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package database

import (
	"database/sql"
	. "lbsas/datatypes"
	"time"

	log "github.com/Sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type IDBMessage interface {
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

func (s *DbHelper) SetCmd(deviceId, cmdType string, cmd *TCMD) {
	s.CmdsList[deviceId+":"+cmdType] = cmd
}

func (s *DbHelper) DeleteCmd(deviceId, cmdType string) {
	delete(s.CmdsList, deviceId+":"+cmdType)
	log.Debug("deleted cmd from ram, deviceId=", deviceId, ", cmdType=", cmdType)
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

	err := s.QueryRow(`select id,deviceId,type,params from commands 
	where status='PENDING' and deviceId=? and type=?`, deviceId, cmdType).Scan(&id_, &deviceId_, &cmdType_, &params_)

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
func New(env *EnviromentCfg) (*DbHelper, error) {
	log.SetLevel(env.LogLevel)
	log.SetFormatter(&log.TextFormatter{})

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

	// set profiling
	helper.Query("SET profiling = ?", func() int {
		if env.DBProf {
			return 1
		} else {
			return 0
		}
	})

	/*
		//populate device id maps
		sqlStr := "select id, deviceImei from device"
		rows, err := helper.Query(sqlStr)
		if err != nil {
			return nil, err
		}

		defer rows.Close()
		var (
			id         string
			deviceImei string
		)
		for rows.Next() {
			err := rows.Scan(&id, &deviceImei)
			if err != nil {
				break
			}
			log.Debug(id, deviceImei)
			helper.IdToImei[id] = deviceImei
			helper.ImeiToId[deviceImei] = id
		}
		err = rows.Err()
		if err != nil {
			log.Error(err, ", SQL:", sqlStr)
		}
	*/

	var refreshCmdsList = func() {
		errSqlStr := "select from commands error:"
		rows, err := helper.Query("select id,deviceId,type,params from commands where status='PENDING'")
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
			//log.Debug("RAM cmd: ", _cmd)
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

			} else {
				helper.CmdsList[deviceId+":"+cmdType] = &TCMD{id, cmdType, params, status}
			}
			//}
		}
		err = rows.Err()
		if err != nil {
			log.Error(errSqlStr, err)
		}

		//		if log.GetLevel() == log.DebugLevel {
		//			for k, v := range helper.CmdsList {
		//				log.Debug("cmds in RAM: ", k, v)
		//			}
		//		}
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
