package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/olivere/elastic/v7"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

type WeiboInfo struct {
	Phone string `json:"phone"`
	Uid string `json:"uid"`
}

func weibo(filePath string) {
	fmt.Println("start processing weibo at", time.Now())
	fd,err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Open file error")
		return
	}
	defer fd.Close()

	esClient, err := elastic.NewClient(elastic.SetURL(esUrl))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	esService, err := esClient.BulkProcessor().
		BulkActions(1000).
		Workers(4).Do(context.Background())
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer esService.Close()

	var lineCount uint64 = 0
	var validCount uint64 = 0
	go func(lineCount *uint64, validCount *uint64) {
		timer := time.Tick(time.Second*5)
		for {
			select {
			case <-timer:
				get, err := esClient.CatCount().Index("weibo").Do(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}
				fmt.Println("around", *lineCount, "lines scanned,", *validCount, "valid records,", get[0].Count, "in database")
			}
		}
	}(&lineCount, &validCount)

	reader := bufio.NewReader(fd)
	phoneExp, err := regexp.Compile("(13[0-9]|14[01456879]|15[0-3,5-9]|16[2567]|17[0-8]|18[0-9]|19[0-3,5-9])")
	if err != nil {
		fmt.Println("regex error", err.Error())
		return
	}

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		//fmt.Println(line)
		splitResult := strings.Split(line, string([]byte{0x09}))
		if len(splitResult) != 2 {
			fmt.Println("error format in line", lineCount, ":", line)
			validCount--
		}
		phone := splitResult[0]
		uid := splitResult[1]
		uid = uid[:len(uid)-2]
		if !phoneExp.MatchString(phone) && lineCount!=0 {
			fmt.Println("invalid phone number in line", lineCount, ":", hex.Dump([]byte(line)))
			lineCount++
			continue
		}
		infoResult := WeiboInfo{Phone: phone, Uid: uid}
		esService.Add(elastic.NewBulkIndexRequest().Index("weibo").Doc(infoResult))
		//fmt.Println(result)
		validCount++
		lineCount++
		//if lineCount > 200000 {
		//	break
		//}
	}
	fmt.Println(lineCount, "lines finish at", time.Now())  // 2h25m12s taken
	fmt.Println("number of valid records:", validCount)  // 503925369
}
