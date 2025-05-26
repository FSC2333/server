package backup_test

import (
	"ECDedup/server/backup"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"testing"
)

func TestStorage(t *testing.T) {
	num := 100000
	datalist := make([][]byte, num)
	gorutineLimit := 200
	semaphore := make(chan struct{}, gorutineLimit)
	var wg sync.WaitGroup
	storage := backup.New(10, 4*1024)
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(i int) {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore
			}()
			defer wg.Done()
			datalist[i] = make([]byte, 4*1024)
			rand.Read(datalist[i])
			storage.Put(intToBytes(i), datalist[i])
		}(i)
	}

	wg.Wait()
	for i := 0; i < num; i++ {
		wg.Add(1)
		go func(i int) {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore
			}()
			defer wg.Done()
			copydata := storage.Get(intToBytes(i))
			hash1 := sha256.Sum256(datalist[i])
			hash2 := sha256.Sum256(copydata)
			if hash1 != hash2 {
				t.Errorf("wrong in compare %d\n", i)
			}
		}(i)
	}
	wg.Wait()
}

func intToBytes(n int) []byte {
	x := int32(n)
	bytesBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytesBuffer, uint32(x))
	return bytesBuffer
}
