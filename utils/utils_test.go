package utils

import "testing"

func TestDecodeTY905Byte(t *testing.T) {
	b1 := byte(0x12)
	b2 := DecodeTY905Byte(b1)
	if b2 != 12 {
		t.Error("expected", 12, "got", b2)
	}

}

func TestEncodeCBCDByte(t *testing.T) {
	b := EncodeCBCDByte("12")
	if b != 0x12 {
		t.Error("expected", 0x12, "got", b)
	}
}

func TestEncodeCBCDFromString(t *testing.T) {
	b := EncodeCBCDFromString("F23A")
	if b[0] != 0xF2 || b[1] != 0x3A {
		t.Error("expected", 0xF23A, "got", b)
	}
}
