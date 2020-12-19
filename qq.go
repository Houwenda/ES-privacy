package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

type QQInfo struct {
	QQNumber string `json:"qq_number"`
	Phone string `json:"phone"`
}

func qq(filePath string)  {
	fmt.Println("start processing qq at", time.Now())
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
		BulkActions(500).
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
				get, err := esClient.CatCount().Index("qq").Do(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}
				if len(get) > 0 {
					fmt.Println("around", *lineCount, "lines scanned,", *validCount, "valid records,", get[0].Count, "in database")
				}
			}
		}
	}(&lineCount, &validCount)

	reader := bufio.NewReader(fd)
	phoneExp, err := regexp.Compile("(13[0-9]|14[01456879]|15[0-3,5-9]|16[2567]|17[0-8]|18[0-9]|19[0-3,5-9])")
	if err != nil {
		fmt.Println("regex error", err.Error())
		return
	}

	var qqNumber string
	var phone string
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		//fmt.Println(line)
		splitResult := strings.Split(line, "----")
		if len(splitResult) == 2 {
			qqNumber = splitResult[0]
			phone = splitResult[1]
			phone = phone[:len(phone)-2]
		} else if len(splitResult) == 3{
			qqNumber = splitResult[1]
			phone = splitResult[2]
			phone = phone[:len(phone)-2]
		} else {
			fmt.Println("error format in line", lineCount, ":", line)
		}

		if !phoneExp.MatchString(phone) && lineCount!=0 {
			fmt.Println("invalid phone number in line", lineCount, ":", line)
			lineCount++
			continue
		}
		if len(qqNumber) < 5 || len(qqNumber) > 11 {
			fmt.Println("invalid qq number in line", lineCount, ":", line)
			lineCount++
			continue
		}
		//result := QQInfo{QQNumber: qqNumber, Phone: phone}
		//fmt.Println(result)
		infoResult := QQInfo{QQNumber: qqNumber, Phone: phone}
		esService.Add(elastic.NewBulkIndexRequest().Index("qq").Doc(infoResult))
		validCount++
		lineCount++
		//if lineCount > 200000 {
		//	break
		//}
	}
	fmt.Println(lineCount, "lines finish at", time.Now())  // 3h40m51s taken
	fmt.Println("number of valid records:", validCount)  // 719618813
}
