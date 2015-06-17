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

type DbHelper struct {
	*sql.DB
	ImeiToId  map[string]string
	IdToImei  map[string]string
	DBMsgChan chan IDBMessage
	// stat
	AvgDBTimeMicroSec, NumDBMsgStored uint64
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
		make(chan IDBMessage, env.DBCacheSize), 0, 0}

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
