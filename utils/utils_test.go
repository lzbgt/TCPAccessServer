package utils

import "testing"

func TestDecodeTY905Byte(t *testing.T) {
	b1 := byte(0x12)
	b2 := DecodeTY905Byte(b1)
	if b2 != 12 {
		t.Error("expected", 12, "got", b2)
	}

}
