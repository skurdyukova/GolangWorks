package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func ExecutePipeline(jobs ...job) {
	var chans [](chan interface{})
	for i := 0; i <= len(jobs); i++ {
		chans = append(chans, make(chan interface{}, MaxInputDataLen))
	}
	wg := &sync.WaitGroup{}
	for i, val := range jobs {
		wg.Add(1)
		go func(in, out chan interface{}, waiter *sync.WaitGroup, val job) {
			defer waiter.Done()
			val(in, out)
			close(out)
		}(chans[i], chans[i+1], wg, val)
	}
	wg.Wait()
}

func SingleHash(in, out chan interface{}) {
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}
	for v1 := range in {
		var data string
		switch x := v1.(type) {
		case int:
			data = strconv.Itoa(x)
		case string:
			data = x
		default:
			fmt.Println("SingleHash I dont know type")
		}
		wg.Add(1)
		go func(data string, waiter *sync.WaitGroup, out chan interface{}) {
			defer waiter.Done()
			ch := make(chan string)
			go func(data string, outCh chan string) {
				outCh <- DataSignerCrc32(data)
				close(outCh)
			}(data, ch)
			mu.Lock()
			md := DataSignerMd5(data)
			mu.Unlock()
			crc2 := DataSignerCrc32(md)
			crc1 := <-ch
			out <- crc1 + "~" + crc2
		}(data, wg, out)
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	wg := &sync.WaitGroup{}
	for v1 := range in {
		data, _ := v1.(string)
		wg.Add(1)
		go func(data string, waiter *sync.WaitGroup, out chan interface{}) {
			defer waiter.Done()
			ch := make(chan [2]string, 6)
			for th := 0; th <= 5; th++ {
				go func(data string, ch chan [2]string, th int) {
					var outData [2]string
					outData[0] = strconv.Itoa(th)
					outData[1] = DataSignerCrc32(strconv.Itoa(th) + data)
					ch <- outData
				}(data, ch, th)
			}
			s := make([]string, 6)
			for i := 0; i <= 5; i++ {
				v := <-ch
				j, _ := strconv.Atoi(v[0])
				s[j] = v[1]
			}
			close(ch)
			out <- strings.Join(s, "")
		}(data, wg, out)
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	var s []string
	for v1 := range in {
		data, _ := v1.(string)
		s = append(s, data)
	}
	sort.Strings(s)
	res := strings.Join(s, "_")
	out <- res
}

func main() {

	testResult := "NOT_SET"

	//inputData := []int{0, 1, 1, 2, 3, 5, 8}
	inputData := []int{0, 1}

	hashSignPipeline := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			<-in
			data, ok := dataRaw.(string)
			if !ok {
				fmt.Println("cant convert result data to string")
			}
			testResult = data
		}),
	}

	start := time.Now()

	ExecutePipeline(hashSignPipeline...)

	end := time.Since(start)

	//expectedTime := 3 * time.Second

	fmt.Println(end, testResult)
}
