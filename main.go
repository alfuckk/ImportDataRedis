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

var rdb = &db.Db{Addr: "localhost:6379", Password: "xxx", DB: 0, PoolSize: "100"}

//任务结构
type Job struct {
	id       int
	filename string
}

//接受数据结构
type Result struct {
	job         Job
	sumofdigits int
}

var (
	wg      sync.WaitGroup
	lock    sync.Mutex
	jobs    = make(chan Job, 10)
	results = make(chan Result, 10)
)

func main() {
	err := rdb.ConnDb()
	if err != nil {
		fmt.Printf("链接数据库错误:%v", err)
	}
	//开启任务数目
	startTime := time.Now()
	noOfWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(noOfWorkers) //执行者数目

	files, _ := ioutil.ReadDir(Path)
	noOfJobs := len(files)
	go queue(noOfJobs, files)
	done := make(chan bool)

	go result(done)
	createWorkerPool(noOfWorkers)
	<-done

	endTime := time.Now()
	diff := endTime.Sub(startTime)
	fmt.Println("total time taken ", diff.Seconds(), "seconds")
}
func queue(noOfJobs int, files []fs.FileInfo) {
	for i := 0; i < noOfJobs; i++ {

		filesuffix := path.Ext(Path + "/" + files[i].Name())
		if filesuffix != ".txt" {
			continue
		}
		file, err := os.Open(Path + "/" + files[i].Name())
		if err != nil {
			fmt.Println("open file failed, err:", err)
			return
		}
		defer file.Close()
		job := Job{id: i, filename: files[i].Name()}
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
		jobs <- job
	}
	close(jobs)
}

func result(done chan bool) {
	for result := range results {
		fmt.Printf("Job id %d,  sum of digits %d\n", result.job.id, result.sumofdigits)
	}
	done <- true
}

func createWorkerPool(noOfWorkers int) {
	var wg sync.WaitGroup
	for i := 0; i < noOfWorkers; i++ {
		wg.Add(1)
		go worker(i, &wg)
	}
	wg.Wait()
	close(results)
}
func worker(i int, wg *sync.WaitGroup) {
	for job := range jobs {
		output := Result{job, job.id}
		results <- output
	}
	fmt.Println(results)
	wg.Done()
}

func explode(s string) []string {
	sli := strings.Split(s, "----")
	if len(sli) != 2 {
		sli = append(sli, "''")
	}
	return sli
}
