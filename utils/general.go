// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package utils

import (
	"errors"
	"math"
	"time"

	log "github.com/Sirupsen/logrus"
)

const SECS_15MINUTE = 15 * 60

// customed time format to time.Time RFC3339, e.g:
// 20150612193050 -> 2015-06-12T19:30:40+00:00
func GetTimestampFromString(tm []byte) time.Time {
	if len(tm) < 14 {
		return time.Now()
	}

	// year
	len := 0
	target := string(tm[:len+4]) + "-"
	len += 4
	// month
	target += string(tm[len:len+2]) + "-"
	len += 2
	// day
	target += string(tm[len:len+2]) + "T"
	len += 2
	//hour
	target += string(tm[len:len+2]) + ":"
	len += 2
	//minute
	target += string(tm[len:len+2]) + ":"
	len += 2
	//sec
	target += string(tm[len:len+2]) + "+00:00"
	len += 2

	t, err := time.Parse(
		time.RFC3339,
		target)
	if err != nil {
		t = time.Now()
		log.Error(err)
	} else if math.Abs(float64(t.Unix()-time.Now().Unix())) > SECS_15MINUTE {
		// TODO: THIS SHOULD BE TAKEN CARED BY THE APPLICATIONS OF GPS DATA, NOT THE ACCESS SERVER ITSELF
		t = time.Now()
	}

	return t
}

func String2LogLevel(strL string) (log.Level, error) {
	var err error = nil
	var lvl log.Level

	switch strL {
	case "debug":
		lvl = log.DebugLevel
	case "info":
		lvl = log.InfoLevel
	case "warn":
		lvl = log.WarnLevel
	case "error":
		lvl = log.ErrorLevel
	case "fatal":
		lvl = log.FatalLevel
	case "panic":
		lvl = log.PanicLevel
	default:
		lvl = log.InfoLevel
		err = errors.New("invalid log level:" + strL)
	}

	return lvl, err
}

func DecodeTY905Byte(bcd byte) byte {
	t1 := bcd & 0x0F
	t2 := (bcd & 0xF0) >> 4

	return t2*10 + t1
}

func DecodeTY905Time(ts []byte) string {
	if len(ts) != 6 {
		return ""
	} else {
		for _, v := range ts {
			v = v
		}
	}
	return ""
}
func DecodeTY905Lat(raw []byte) float32 {
	return 0
}

func EncodeCBCDByte(str string) byte {
	ret := byte(0)
	if len(str) != 2 {
		//return 0
	} else {
		for _, v := range []byte(str) {
			ret = ret << 4
			if v >= '0' && v <= '9' {
				ret = ret | (v - '0')
			} else if v >= 'A' && v <= 'F' {
				ret = ret | (v - 'A' + 10)
			} else if v >= 'a' && v <= 'f' {
				ret = ret | (v - 'a' + 10)
			}
		}
	}

	return ret
}

func EncodeCBCDFromString(str string) []byte {
	empty := make([]byte, 0)

	if len(str)%2 != 0 {
		return empty
	}

	ret := make([]byte, len(str)/2)
	for i := 0; i < len(str)/2; i++ {
		index := 2 * i
		ret[i] = EncodeCBCDByte(str[index : index+2])

	}

	return ret

}
