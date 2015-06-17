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
	"lbsas/utils"
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
	log.Info("Server Started")
	tcp.New(nbsihai.New(env))

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	log.Info("program is safely shutting down")
}

// handle command line args
func GetEnvCfg() *EnviromentCfg {
	env := &EnviromentCfg{}

	var lvl log.Level
	flagLvl := flag.String("log", "info", "log level")
	flagMaxOpenConns := flag.Int("dbmoc", 400, "database max open connections")
	flagMaxIdleConns := flag.Int("dbmic", 100, "database max idle connections")
	flagTCPTimeOutSec := flag.Int("tcptimeout", 200, "tcp wait time out, seconds")
	flagTCPAddr := flag.String("tcpaddr", "0.0.0.0:8082", "TCP addr of server, like 0.0.0.0:8082")
	flagHTTPAddr := flag.String("httpaddr", "0.0.0.0:8083", "HTTP addr of server, like 0.0.0.0:8082")
	flagQueSize := flag.Int("queue", 80, "queue size per tcp connection")
	flagWorkers := flag.Int("worker", 1, "num of workers per tcp connection")
	flagDBAddr := flag.String("dbaddr", "root:tusung*123@tcp(192.168.1.3:3306)/cargts", "database address")
	flagDBProf := flag.Bool("dbprof", false, "enable database profiling")
	flagDBCacheSize := flag.Int64("dbcachesize", 8000000, "dbmessage cache size before saving to database")
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
	env.DBProf = *flagDBProf
	env.DBCacheSize = *flagDBCacheSize

	return env
}
