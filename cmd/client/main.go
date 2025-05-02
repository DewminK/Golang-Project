package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

type cmdArgs struct {
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func main() {
	addr := flag.String("addr", "localhost:9002", "leader address")
	flag.Parse()
	if flag.NArg() < 2 {
		fmt.Println("usage: client [get|put|del] key [value]")
		os.Exit(1)
	}
	op := flag.Arg(0)
	key := flag.Arg(1)
	var val string
	if op == "put" {
		if flag.NArg() != 3 {
			fmt.Println("put requires value")
			os.Exit(1)
		}
		val = flag.Arg(2)
	}
	reqBody, _ := json.Marshal(cmdArgs{Key: key, Value: val})
	url := fmt.Sprintf("http://%s/%s", *addr, op)
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}
