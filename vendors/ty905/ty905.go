// Copyright 2015 ZheJiang QunShuo, Inc. All rights reserved
//
// History:
// 2015-06-06	Bruce.Lu<rikusouhou@gmail.com>  Initial version
//

package ty905

import (
	"encoding/hex"
	"fmt"
	"lbsas/datatypes"
)

const (
	MSG_HEAD = "\x29\x29"
	MSG_TAIL = "\x0D"

	MSG_CMD_UP_NORM_GEO   = "\x80"
	MSG_CMD_UP_TIME_PROTO = "\xD6"
	MSG_CMD_UP_ACK        = "\x85"

	MSG_CMD_DOWN_REP = "\x21"
	MSG_CMD_DOWN_CFG = "\x7B"
	MSG_CMD_DOWN_MSG = "\x3A"

	GEO_DATA_LEN = 34
)

type TY905 struct {
	rawPacket *datatypes.RawUdpPacket
}

func New(rp *datatypes.RawUdpPacket) *TY905 {
	return &TY905{rp}
}

func (s *TY905) Valid() bool {
	return true
}

func (s *TY905) Srv() {
	if !s.Valid() {
		fmt.Println("Invalid packet")
		return
	}

	fmt.Println(hex.EncodeToString(s.rawPacket.Buff[0:s.rawPacket.Size]), s.rawPacket.Size)
	s.rawPacket.UdpConn.WriteToUDP(s.rawPacket.Buff[0:s.rawPacket.Size],
		s.rawPacket.Remote)

}
