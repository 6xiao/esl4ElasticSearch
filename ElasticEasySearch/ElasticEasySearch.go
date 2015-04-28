package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/6xiao/esl4ElasticSearch/EasySearch"
	"github.com/6xiao/go/Common"
	es "github.com/mattbaird/elastigo/lib"
	"log"
	"runtime"
)

var (
	flgRpc = flag.String("rpc", ":12311", "rpc server port")
	flgEs  = flag.String("elasticsearch", "192.168.248.16", "elastic search")
)

func Search(ess EasySearch.EasySearch, res *map[string][]byte) error {
	defer Common.CheckPanic()

	filter, err := ParseEsl(ess.ESL)
	if filter == nil || err != nil {
		log.Println("error parse esl :", err, ess.ESL)
		return err
	}

	switch filter.(type) {
	case *es.FilterWrap:
		b, e := filter.(*es.FilterWrap).MarshalJSON()
		fmt.Println("filter:", string(b), e)

	case *es.FilterOp:
		b, e := es.CompoundFilter(filter).MarshalJSON()
		fmt.Println("filter:", string(b), e)

	default:
		return errors.New("esl parse error")
	}

	c := es.NewConn()
	c.Domain = *flgEs
	se := es.Query().Term("appkey", ess.Appkey)
	re := es.Search(ess.Index).Type(ess.Type).Fields(ess.Fields...)
	rsp, err := re.Size("65536").Scroll("1").Query(se).Filter(filter).Result(c)
	if err != nil {
		log.Println("error:", err)
		return err
	}

	param := make(map[string]interface{})
	param["scroll"] = "1"

	for len(*res) < rsp.Hits.Total && len(rsp.Hits.Hits) > 0 {
		for _, item := range rsp.Hits.Hits {
			if item.Fields != nil {
				(*res)[item.Id], _ = item.Fields.MarshalJSON()
			} else if item.Source != nil {
				(*res)[item.Id], _ = item.Source.MarshalJSON()
			} else {
				(*res)[item.Id] = nil
			}
		}

		*rsp, err = c.Scroll(param, rsp.ScrollId)
		if err != nil {
			log.Println("error:", err)
			return err
		}
	}

	return nil
}

func init() {
	flag.Parse()
	flag.Usage()
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {
	trpc := EasySearch.NewEsRpc(Search)
	Common.ListenRpc(*flgRpc, trpc)
	log.Fatal("exit ...")
}
