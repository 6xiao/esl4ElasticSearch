package EasySearch

import (
	"errors"
	"log"
	"net"
	"net/rpc"
)

type EasySearch struct {
	Appkey   string
	Platform string
	Index    string
	Type     string
	Fields   []string
	ESL      string
	SysCond  string
}

type SearchInvoke func(EasySearch, *map[string][]byte) error

type ESRPC struct {
	Address    string
	SearchFunc SearchInvoke
}

func NewEsRpc(addr string, sf SearchInvoke) *ESRPC {
	return &ESRPC{addr, sf}
}

func (es *ESRPC) Search(in EasySearch, out *map[string][]byte) error {
	if es.SearchFunc != nil {
		*out = make(map[string][]byte)
		return es.SearchFunc(in, out)
	}

	return errors.New("can't search")
}

func ListenEsRpc(esrpc *ESRPC) {
	defer log.Fatal("Big Error : Rpc Server quit")
	defer CheckPanic()

	server := rpc.NewServer()
	server.Register(esrpc)

	if listener, err := net.Listen("tcp", esrpc.Address); err != nil {
		log.Println("Big Error : Rpc can't start", err)
	} else {
		log.Println("Task Rpc running @", esrpc.Address)
		server.Accept(listener)
	}
}

func CheckPanic() {
	if err := recover(); err != nil {
		log.Panicln("Panic : ", err)
	}
}
