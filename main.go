package main

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/iceber/iouring-go"
)

func assert(b bool) {
	if !b {
		panic("assert")
	}
}

const BUFFER_SIZE = 4096

func readNBytes(fn string, n int) []byte {
	f, err := os.Open(fn)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	data := make([]byte, 0, n)

	var buffer = make([]byte, BUFFER_SIZE)
	for len(data) < n {
		read, err := f.Read(buffer)
		if err != nil {
			panic(err)
		}

		data = append(data, buffer[:read]...)
	}

	assert(len(data) == n)

	return data
}

func benchmark(name string, data []byte, fn func(*os.File)) {
	fmt.Printf("%s", name)
	f, err := os.OpenFile("out.bin", os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0755)
	if err != nil {
		panic(err)
	}

	t1 := time.Now()

	fn(f)

	s := time.Now().Sub(t1).Seconds()
	fmt.Printf(",%f,%f\n", s, float64(len(data))/s)

	if err := f.Close(); err != nil {
		panic(err)
	}

	assert(bytes.Equal(readNBytes("out.bin", len(data)), data))
}

func main() {
	size := 104857600 // 100MiB
	data := readNBytes("/dev/random", size)

	const RUNS = 10
	for i := 0; i < RUNS; i++ {
		benchmark("blocking", data, func(f *os.File) {
			for i := 0; i < len(data); i += BUFFER_SIZE {
				size := min(BUFFER_SIZE, len(data)-i)
				n, err := f.Write(data[i : i+size])
				if err != nil {
					panic(err)
				}

				assert(n == BUFFER_SIZE)
			}
		})

		benchmarkIOUringNEntries := func (nEntries int) {
			benchmark(fmt.Sprintf("io_uring_%d_entries", nEntries), data, func(f * os.File) {
				iour, err := iouring.New(uint(nEntries))
				if err != nil {
					panic(err)
				}
				defer iour.Close()

				requests := make([]iouring.PrepRequest, nEntries)

				for i := 0; i < len(data); i += BUFFER_SIZE * nEntries {
					submittedEntries := 0
					for j := 0; j < nEntries; j++ {
						base := i + j * BUFFER_SIZE
						if base >= len(data) {
							break
						}
						submittedEntries++
						size := min(BUFFER_SIZE, len(data)-i)
						requests[j] = iouring.Pwrite(int(f.Fd()), data[base : base+size], uint64(base))
					}

					if submittedEntries == 0 {
						break
					}

					res, err := iour.SubmitRequests(requests[:submittedEntries], nil)
					if err != nil {
						panic(err)
					}

					<-res.Done()

					for _, result := range res.ErrResults() {
						_, err := result.ReturnInt()
						if err != nil {
							panic(err)
						}
					}
				}
			})
		}
		benchmarkIOUringNEntries(1)
		benchmarkIOUringNEntries(128)
	}
}
