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

var (
	MSG_HEAD = []byte("\x29\x29")
	MSG_TAIL = []byte("\x0D")

	MSG_CMD_UP_NORM_GEO = []byte("\x80")
	MSG_CMD_DOWN_REP    = []byte("\x21")

	LBS_DATA_LEN = 34
	ID_LEN       = 4
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
