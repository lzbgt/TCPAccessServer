package main

import (
	"fmt"
	"net"
	"os"
	"io/ioutil"
	"time"
	"github.com/vaughan0/go-ini"
	"strconv"
	"strings"
	"os/signal"
	"syscall"
)

var GlobalCfg, _ = ini.LoadFile("config.ini")

type Point struct {
	lat float64
	lon float64
	speed float64
	heading float64
}

func readAll(filePath string) ([]byte, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	
	return ioutil.ReadAll(f)
}

func initRoute(filePath string) ([]Point, error) {
	bytes, err := readAll(filePath)
	if err != nil {
		return nil, err
	}
	
	route := string(bytes)
	points := strings.Split(route, ";")
	pos := make([]Point, len(points), len(points))
	for index, po := range points {
		values := strings.Split(po, ",")
		if len(values) < 5 {
			fmt.Println("unknown format")
			continue
		}
		pos[index].lon, _ = strconv.ParseFloat(values[0], 32)
		pos[index].lat, _ = strconv.ParseFloat(values[1], 32)
		pos[index].speed, _ = strconv.ParseFloat(values[3], 32)
		pos[index].heading, _ = strconv.ParseFloat(values[4], 32)
	}
	return pos, err
}

func genMessage(deviceImei string, po Point) string {
	//GTSTR,110204,135790246811220,GL500,0,0,0,25.0,81,0,0.1,0,0.3,121.390875,31.164600,20130312183936,0460,0000,1877,0873,,,,20130312190551,0304$
	formatStr := "+RESP:GTSTR,110204,%s,GL500,0,0,0,25.0,81,0,%f,%f,0.3,%f,%f,%s,0460,0000,1877,0873,,,,%s,0304$"
	time := GetUTCDate()
	s := fmt.Sprintf(formatStr, deviceImei, po.speed, po.heading, po.lon, po.lat, time, time)
	return s
}

func main() {
	server, ok := GlobalCfg.Get("COMMON", "server")
	if !ok {
		fmt.Println("server ip/port not set")
		return
	}
	
	_startImei, ok := GlobalCfg.Get("COMMON", "start_imei")
	if !ok {
		fmt.Println("start imei not set")
		return
	}
	startImei, _ := strconv.ParseInt(_startImei, 10, 64)
	
	deviceNum, ok := GlobalCfg.Get("COMMON", "device_number")
	if !ok {
		fmt.Println("device number not set")
		return
	}
	threadNum, err := strconv.ParseInt(deviceNum, 10, 64)
	if err != nil {
		fmt.Println(err)
	}
	
	pos, err := initRoute("route.txt")
	if err != nil {
		fmt.Println("failed to init route")
		return
	}
	
	for i := int64(0); i < threadNum; i++ {
		deviceImei := startImei + i
		fmt.Println(deviceImei)
		go func(deviceImei int64) {
			tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
				os.Exit(1)
			}
			
			conn, err := net.DialTCP("tcp", nil, tcpAddr)
			if(err != nil) {
				fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
				os.Exit(1)
			}
			
			defer conn.Close()
			fmt.Println("connect success")
			
			poLen := len(pos)
			po := 0
			for {
				if po >= poLen {
					po = 0
				}
				strDeviceImei := fmt.Sprint(deviceImei)
				msg := genMessage(strDeviceImei, pos[po])
				fmt.Println(msg)
				conn.Write([]byte(msg))
				time.Sleep(10*time.Second)
				
				po = po + 1
			}
		}(deviceImei)
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
}