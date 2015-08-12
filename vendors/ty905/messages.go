package ty905

type Message struct {
	MsgHead, // 2
	MajorCmd, // 1
	Length, //2
	IP, // 4
	Content, // n
	CheckSum, // 1
	MsgTail []byte //1
}

type IMessage interface {
}
