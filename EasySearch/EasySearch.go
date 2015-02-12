package EasySearch

import (
	"errors"
)

type EasySearch struct {
	Appkey string
	Index  string
	Type   string
	Fields []string
	ESL    string
}

type SearchInvoke func(EasySearch, *map[string][]byte) error

type ESRPC struct {
	SearchFunc SearchInvoke
}

func NewEsRpc(sf SearchInvoke) *ESRPC {
	return &ESRPC{sf}
}

func (es *ESRPC) Search(in EasySearch, out *map[string][]byte) error {
	if es.SearchFunc != nil {
		*out = make(map[string][]byte)
		return es.SearchFunc(in, out)
	}

	return errors.New("can't search")
}
