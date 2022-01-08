package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
// optimize 1:	In the 1st round Map, merge the same key -> reducing I/O
// optimize 2: In the 1st round Reduce, sum up the frequency of each key
// optimize 3: In the 2rd round Map, sort and filter out the ones ranked after 10th

func URLTop10(nWorkers int) RoundsArgs {
	var args RoundsArgs
	args = append(args, RoundArgs{
		MapFunc:	URLCountMap,
		ReduceFunc:	URLCountReduce,
		NReduce:	nWorkers,
	})
	args = append(args, RoundArgs{
		MapFunc:	URLTop10Map,
		// MapFunc:	ExampleURLTop10Map,
		ReduceFunc:	URLTop10Reduce,
		NReduce:	1,
	})
	return args
}

func URLCountMap (filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvMap := make(map[string]int)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		if _, exists := kvMap[l]; !exists {
			kvMap[l] = 0
		}
		kvMap[l] += 1
	}
	kvs := make([]KeyValue, 0)
	for k, v := range kvMap {
		kvs = append(kvs, KeyValue{Key: k, Value: strconv.Itoa(v)})
	}
	return kvs
}

func URLCountReduce (key string, values []string) string {
	count := 0
	for _, v := range values {
		v, err := strconv.Atoi(v)
		if err != nil {
			panic(err)
		}
		count += v
	}
	return fmt.Sprintf("%s: %d\n", key, count)
}

func URLTop10Map (filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvMap := make(map[string]int)
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		tmp := strings.Split(l, ": ")
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		kvMap[tmp[0]] = n
	}
	us, cs := TopN(kvMap, 10)
	kvs := make([]KeyValue, 0, 10)
	for i := range us {
		kvs = append(kvs, KeyValue{
			Key: "",
			Value: fmt.Sprintf("%s: %d", us[i], cs[i]),
		})
	}
	return kvs
}


func URLTop10Reduce (key string, values []string) string {
	kvMap := make(map[string]int, len(values))
	for _, value := range values {
		tmp := strings.Split(value, ": ")
		url := strings.TrimSpace(tmp[0])
		if len(url) == 0 {
			continue
		}
		n, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		kvMap[url] = n
	}
	us, cs := TopN(kvMap, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}

