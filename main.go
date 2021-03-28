package main

import (
	"bufio"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/mishupaf-create/ImportDataRedis/db"
	uuid "github.com/satori/go.uuid"
)

const (
	Path = "/Users/xxuechin/Downloads"
)

var rdb = &db.Db{Addr: "localhost:6379", Password: "xxxxx"}

//任务结构
type Job struct {
	id       int
	filename string
}

var (
	wg   sync.WaitGroup
	lock sync.Mutex
	jobs = make(chan Job, 10)
)

func main() {
	//开启任务数目
	startTime := time.Now()
	noOfWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(noOfWorkers) //执行者数目

	files, _ := ioutil.ReadDir(Path)
	noOfJobs := len(files)
	go queue(noOfJobs, files) //并发的开几个任务
	done := make(chan bool)
	<-done

	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}

//单个文件处理
func worker(filename string) {
	file, err := os.Open(Path + "/" + filename)
	if err != nil {
		fmt.Println("open file failed, err:", err)
		return
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		uid := uuid.NewV4().String()
		lines, err := reader.ReadString('\n') //注意是字符
		substr := explode(lines)              //正常数据流
		if err == io.EOF {
			if len(lines) != 0 {
				//读完数据流
				rdb.Set(uid, map[string]interface{}{"qq": substr[0], "mobile": substr[1], "uid": uid})
			}
			break
		}
		if err != nil {
			fmt.Println("read file failed, err:", err)
			return
		}
		rdb.Set(uid, map[string]interface{}{"qq": substr[0], "mobile": substr[1], "uid": uid})
		fmt.Println(lines)
	}
	wg.Done()

}
func queue(noOfJobs int, files []fs.FileInfo) {
	for i := 0; i < noOfJobs; i++ {
		err := rdb.ConnDb(i) //每个文件一个DB 有多少任务就使用多少db
		if err != nil {
			fmt.Printf("链接数据库错误:%v", err)
		}
		filesuffix := path.Ext(Path + "/" + files[i].Name())
		if filesuffix != ".txt" {
			continue
		}
		wg.Add(1)
		lock.Lock()
		go worker(files[i].Name())
		lock.Unlock()

		job := Job{id: i, filename: files[i].Name()}
		jobs <- job
	}
	wg.Wait()
	close(jobs)
}

func explode(s string) []string {
	sli := strings.Split(s, "----")
	if len(sli) != 2 {
		sli = append(sli, "''")
	}
	return sli
}
