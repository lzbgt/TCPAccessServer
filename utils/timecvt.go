// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package utils

import (
	"math"
	"time"
)
import log "github.com/Sirupsen/logrus"

const TWO_DAY_SECS = 2 * 24 * 60 * 60

func GetTimestampFromString(tm []byte) time.Time {
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
	} else if math.Abs(float64(t.Unix()-time.Now().Unix())) > TWO_DAY_SECS {
		t = time.Now()
	}

	return t
}
