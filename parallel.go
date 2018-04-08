package jsoniterll

import (
	"context"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var jcf = jsoniter.ConfigCompatibleWithStandardLibrary

type iVal struct {
	i   int
	val interface{}
}

type iRes struct {
	i    int
	data []byte
	err  error
}

func MarshalParallel(workerCnt int, bulk []interface{}) ([][]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // make sure all paths cancel the context to avoid context leak

	jobs := make(chan iVal)

	go func() {
		defer close(jobs)

		for i, val := range bulk {
			select {
			case <-ctx.Done():
				return
			case jobs <- iVal{i, val}:
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workerCnt)

	results := make(chan iRes)
	for i := 0; i < workerCnt; i++ {
		go func() {
			defer wg.Done()

			marshalWorker(ctx, jobs, results)
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	dataSlice := make([][]byte, len(bulk))
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		dataSlice[res.i] = res.data
	}

	return dataSlice, nil
}

func marshalWorker(ctx context.Context, in <-chan iVal, out chan<- iRes) {
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-in:
			if !ok {
				return
			}
			res := iRes{i: job.i}
			if data, err := marshal(job.val); err != nil {
				res.err = err
			} else {
				res.data = data
			}
			out <- res
		}
	}
}

func marshal(v interface{}) ([]byte, error) {
	// See https://github.com/json-iterator/go/blob/master/example_test.go#L47-L67
	stream := jcf.BorrowStream(nil)
	defer jcf.ReturnStream(stream)

	stream.WriteVal(v)

	if stream.Error != nil {
		return nil, stream.Error
	}

	return stream.Buffer(), nil
}
