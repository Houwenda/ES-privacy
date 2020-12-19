package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/olivere/elastic/v7"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

var mailExp *regexp.Regexp

type JDInfo struct {
	Name string `json:"name"`
	NickName string `json:"nickname"`
	PasswordHash string `json:"password_hash"`
	Mail string `json:"mail"`
	ID string `json:"id"`
	Phone1 string `json:"phone1"`
	Phone2 string `json:"phone2"`
}

func jd(filePath string) {
	fmt.Println("start processing jd at", time.Now())
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
				get, err := esClient.CatCount().Index("jd").Do(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}
				fmt.Println("around", *lineCount, "lines scanned,", *validCount, "valid records,", get[0].Count, "in database")
			}
		}
	}(&lineCount, &validCount)

	reader := bufio.NewReader(fd)
	mailExp, err = regexp.Compile("[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+")
	if err != nil {
		fmt.Println("regex error", err.Error())
		return
	}
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		infoResult, err := extractJD(line)
		if err != nil {
			//fmt.Println(err.Error(), lineCount, line)
			lineCount++
			continue
		}
		esService.Add(elastic.NewBulkIndexRequest().Index("jd").Doc(infoResult))

		lineCount++
		validCount++
		//if lineCount>200 {
		//	break
		//}
	}
	fmt.Println(lineCount, "lines finish at", time.Now())  // 60m10s
	fmt.Println("number of valid records:", validCount)  // 141599361
}

func extractJD(line string) (JDInfo, error){
	//fmt.Println(line)
	splitResult := strings.Split(line, "---")
	if len(splitResult) != 7 {
		//fmt.Println(len(splitResult), line)
		return JDInfo{}, errors.New("invalid number of columns in line: ")
	}

	result := JDInfo{
		Name:         splitResult[0],
		NickName:     splitResult[1],
		PasswordHash: splitResult[2],
		Mail:         splitResult[3],
		ID:           splitResult[4],
		Phone1:       strings.TrimSpace(splitResult[5]) ,
		Phone2:       strings.TrimSpace(splitResult[6]),
	}
	if result.Name == "\\N" {
		result.Name = ""
	}
	if result.NickName == "\\N" {
		result.NickName = ""
	}
	if !mailExp.MatchString(result.Mail) {
		result.Mail = ""
	}
	if len(result.ID) != 18 {
		result.ID = ""
	}
	if len(result.Phone1) < 8 || len(result.Phone1) > 16 {
		result.Phone1 = ""
	}
	if len(result.Phone2) < 8 {
		result.Phone2 = ""
	}
	return result, nil
}