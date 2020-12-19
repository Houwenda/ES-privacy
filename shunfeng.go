package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/olivere/elastic/v7"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"io"
	"os"
	"regexp"
	"time"
)

var (
	fieldExp *regexp.Regexp
)

type ShunfengInfo struct {
	Name     string `json:"name"`
	Phone    string `json:"phone"`
	Province string `json:"province"`
	City     string `json:"city"`
	Dist     string `json:"dist"`
	Addr     string `json:"addr"`
}

func shunfeng(filePath string) {
	fmt.Println("start processing shunfeng at", time.Now())
	fd,err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("Open file error")
		return
	}
	defer fd.Close()

	utf16bom := unicode.BOMOverride(unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder())
	utf16Reader := transform.NewReader(fd, utf16bom)
	reader := bufio.NewReader(utf16Reader)

	//fieldExp, err = regexp.Compile("N'[\\w,\\-,\\*,/,:,(,),#,~,,\u4E00-\u9FFF,\u2014,\uFF0C,\uFF08,\uFF1A,\u3001-\u300B,\uFF09,\u3400-\u4DBF]*'")
	fieldExp, err = regexp.Compile("N'[^']*'")
	if err != nil {
		fmt.Println("regex error", err.Error())
		return
	}

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
				get, err := esClient.CatCount().Index("shunfeng").Do(context.Background())
				if err != nil {
					fmt.Println(err.Error())
				}
				fmt.Println("around", *lineCount, "lines scanned,", *validCount, "valid records,", get[0].Count, "in database")
			}
		}
	}(&lineCount, &validCount)

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if lineCount >= 20 && len(line) > 80{
			infoResult, err := extractShunfeng(line)
			if err != nil {
				fmt.Println(err.Error(), lineCount+20)
				// return
			}
			esService.Add(elastic.NewBulkIndexRequest().Index("shunfeng").Doc(infoResult))
			validCount++
			//fmt.Println(infoResult)
		}

		//if lineCount > 4000000 {
		//	break
		//}
		lineCount++
	}
	fmt.Println(lineCount, "lines finish at", time.Now())  // 22m23s taken to write all
	fmt.Println("number of valid records:", validCount)  // 65990347
}

func extractShunfeng(insertLine string) (ShunfengInfo, error) {
	//fmt.Print(insertLine)
	expResult := fieldExp.FindAllString(insertLine, 7)
	var result ShunfengInfo
	if len(expResult) != 6 {
		fmt.Print(insertLine, expResult)
		return result, errors.New("error number of fields in insert line: ")
	}
	if len(expResult[0]) > 3 {
		result.Name = expResult[0][2:]
		result.Name = result.Name[:len(result.Name)-1]
		//fmt.Print("this record has none-empty name field: ", insertLine)
	}
	if len(expResult[1]) > 3 {
		result.Phone = expResult[1][2:]
		result.Phone = result.Phone[:len(result.Phone)-1]
	}
	if len(expResult[2]) > 3 {
		result.Province = expResult[2][2:]
		result.Province = result.Province[:len(result.Province)-1]
	}
	if len(expResult[3]) > 3 {
		result.City = expResult[3][2:]
		result.City = result.City[:len(result.City)-1]
	}
	if len(expResult[4]) > 3 {
		result.Dist = expResult[4][2:]
		result.Dist = result.Dist[:len(result.Dist)-1]
	}
	if len(expResult[5]) > 3 {
		result.Addr = expResult[5][2:]
		result.Addr = result.Addr[:len(result.Addr)-1]
	}

	if len(result.Province) == 11 {
		result.Phone = result.Province
		result.Province = result.City
		result.City = result.Dist
		result.Dist = result.Addr
		result.Addr = ""
	}
	return result, nil
}

