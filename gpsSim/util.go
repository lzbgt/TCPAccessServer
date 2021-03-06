package main

import (
	"fmt"
	"strconv"
	"time"
)

func padding(value int) string {
	if value < 10 {
		return "0" + strconv.Itoa(value)
	} else {
		return strconv.Itoa(value)
	}
}

func monthStr(m time.Month) string {
	switch m {
	case time.January:
		return "01"
	case time.February:
		return "02"
	case time.March:
		return "03"
	case time.April:
		return "04"
	case time.May:
		return "05"
	case time.June:
		return "06"
	case time.July:
		return "07"
	case time.August:
		return "08"
	case time.September:
		return "09"
	case time.October:
		return "10"
	case time.November:
		return "11"
	case time.December:
		return "12"
	default:
		return "01"
	}
}

func GetUTCDate() string {
	t := time.Now().UTC()
	s := fmt.Sprintf("%s%s%s%s%s%s", padding(t.Year()), monthStr(t.Month()), padding(t.Day()), padding(t.Hour()), padding(t.Minute()), padding(t.Second()))
	return s
}

func main_test() {
	t := time.Now()
	s := fmt.Sprintf("%s%s%s%s%s%s", padding(t.Year()), monthStr(t.Month()), padding(t.Day()), padding(t.Hour()), padding(t.Minute()), padding(t.Second()))
	fmt.Println(s)
}
