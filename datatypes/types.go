// Copyright 2015 ZheJiang QunShuo Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package datatypes

import (
	"errors"
	"net"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"
)

var ErrorMessage = map[string]error{
	"INVALID_PACKET_HT":  errors.New("start/end symbol in packet is invalid"),
	"INVALID_PACKET_LEN": errors.New("invalid packet length or num of fields"),
}

type RawUdpPacket struct {
	Buff    []byte
	Remote  *net.UDPAddr
	UdpConn *net.UDPConn
}

type RawTcpPacket struct {
	Buff []byte
	Conn *net.Conn
}

// command line args
type EnviromentCfg struct {
	DBMaxOpenConns, DBMaxIdleConns,
	QueueSizePerConn, NumWorkersPerConn, TCPTimeOutSec, NumUDPWokers int
	LogLevel                          log.Level
	TCPAddr, HTTPAddr, DBAddr, LbsUrl string
	DBCacheSize, MsgCacheSize         int64

	DType string
}

// vendor
type Vendor interface {
	GetCfg() *NetConfig
	SetLogLevel(log.Level)
	Close()
	TcpWorker(packetsChan chan *RawTcpPacket)
	IsWholePacket(buff []byte, status *int) (bool, error)
}

// stat
type VendorStat struct {
	AvgWorkerTimeMicroSec, DBWriteMsgCacheSize, DBWriteMsgDropped uint64
	NumInvalidPackets, AvgDBTimeMicroSec, NumDBMsgStored          uint64
}

// tcp configurations
type NetConfig struct {
	// config
	Addr, Protocol, HttpAddr, DBAddr  string
	StartSymbol, EndSymbol            byte
	ChanSize, PacketMaxLen, WorkerNum int
	ReadTimeoutSec                    time.Duration
	LogLevel                          log.Level
}

// framework statistics
type NetStatus struct {
	NumConnActive, NumConnCreated, NumConnClosed, NumPktsReceived, NumErrorRcv, NumDBMsgStored,
	NumPktsDroped, NumInvalidPkts uint64
	NumConnCreatedPS, NumConnClosedPS, NumPktsReceivedPS, NumErrorRcvPS, NumDBMsgStoredPS,
	NumPktsDropedPS, NumInvalidPktsPS,
	MaxNumConnCreatedPS, MaxNumConnClosedPS, MaxNumPktsReceivedPS, MaxNumErrorRcvPS, MaxNumDBMsgStoredPS,
	MaxNumPktsDropedPS, MaxNumInvalidPktsPS, NumDBWriteMsgDroppedPS float32
	NumInvalidPackets, AvgWorkerTimeMicroSec, AvgDBTimeMicroSec, NumDBWriteMsgCacheSize, NumDBWriteMsgDropped uint64

	StartTime, LastTime, NowTime time.Time

	//
	MemStat runtime.MemStats
}

type WSGLocation struct {
	Lat string `json:"lat"`
	Lon string `json:"lon"`
}
type LBSLocation struct {
	Lat   float64 `json:"lat"`
	Lon   float64 `json:"lon"`
	Power byte    `json:"power"`
	TA    byte    `json:"ta"`
}
