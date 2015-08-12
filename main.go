// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package main

import (
	"flag"
	. "lbsas/datatypes"
	"lbsas/tcp"
	"lbsas/udp"
	"lbsas/utils"
	"lbsas/vendors/eworld"
	"lbsas/vendors/gl500/nbsihai"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	log "github.com/Sirupsen/logrus"
)

//
func main() {
	env := GetEnvCfg()
	// unless special case, always use max CPU cores available
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevel(env.LogLevel)
	log.SetFormatter(&log.TextFormatter{})

	// start a new tcp server for Battery Powered GPS Devices
	log.Info("Configurations:", env)
	log.Info("Starting the server ...")
	if env.DType == "gl500" {
		tcp.New(nbsihai.New(env))
	} else if env.DType == "eworld" {
		tcp.New(eworld.New(env))
	} else if env.DType == "ty905" {
		udp.New(*env)
	}

	log.Info("Server Started")

	// accept SIGTERM signal for safely exiting
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Info("program is safely shutting down")
}

// handle command line args
func GetEnvCfg() *EnviromentCfg {
	env := &EnviromentCfg{}

	var lvl log.Level
	flagLvl := flag.String("log", "error", "log level")
	flagLbsUrl := flag.String("lbs", "http://127.0.0.1:8010/api/lbs", "lbs api url")
	flagType := flag.String("dtype", "eworld", "device type:gl500, eworld, ty905")
	flagMaxOpenConns := flag.Int("dbmoc", 400, "database max open connections")
	flagMaxIdleConns := flag.Int("dbmic", 100, "database max idle connections")
	flagTCPTimeOutSec := flag.Int("rdto", 90, "read time out, seconds")
	flagTCPAddr := flag.String("srvaddr", "0.0.0.0:8082", "UDP/TCP addr of server, like 0.0.0.0:8082")
	flagHTTPAddr := flag.String("httpaddr", "0.0.0.0:8083", "HTTP addr of server, like 0.0.0.0:8082")
	flagQueSize := flag.Int("queue", 800, "queue size per tcp connection")
	flagWorkers := flag.Int("worker", 1, "num of workers per tcp connection")
	flagDBAddr := flag.String("dbaddr", "root:tusung*123@tcp(192.168.1.3:3306)/cargts", "database address")
	flagDBCacheSize := flag.Int64("dbcachesize", 800000, "dbmessage cache size before saving to database")
	flag.Parse()

	lvl, _ = utils.String2LogLevel(*flagLvl)
	env.LogLevel = lvl
	env.DBMaxOpenConns = *flagMaxOpenConns
	env.DBMaxIdleConns = *flagMaxIdleConns
	env.QueueSizePerConn = *flagQueSize
	env.NumWorkersPerConn = *flagWorkers
	env.TCPTimeOutSec = *flagTCPTimeOutSec
	env.TCPAddr = *flagTCPAddr
	env.HTTPAddr = *flagHTTPAddr
	env.DBAddr = *flagDBAddr
	env.DBCacheSize = *flagDBCacheSize
	env.DType = *flagType
	env.LbsUrl = *flagLbsUrl

	return env
}
