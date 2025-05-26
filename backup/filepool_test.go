package backup_test

import (
	"ECDedup/server/backup"
	"crypto/sha256"
	"math/rand"
	"sync"
	"testing"
)

func TestFilepool(t *testing.T) {
	// backup.initFilepool()
	num := 100000
	filepool := backup.NewFilepool(10, 64*1024)
	dataList := make([][]byte, num)
	indexList := make([]*backup.DataIndex, num)
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		dataList[i] = make([]byte, 4*1024)
		rand.Read(dataList[i])
		wg.Add(1)
		go func(index int) {
			fileNo, offset, datasize := filepool.WriteToDisk(dataList[index])
			indexList[index] = &backup.DataIndex{FileNo: fileNo, Offset: offset, Datasize: datasize}
			wg.Done()
		}(i)
	}
	wg.Wait()
	for index := 0; index < num; index++ {

		copydata := filepool.ReadFromDisk(indexList[index].FileNo, indexList[index].Offset, indexList[index].Datasize)
		hash1 := sha256.Sum256(dataList[index])
		hash2 := sha256.Sum256(copydata)
		if hash1 != hash2 {
			t.Errorf("wrong in %d", index)
		}

	}
	// for i := 0; i < num; i++ {
	// 	go func(index int) {
	// 		copydata := filepool.ReadFromDisk(indexList[index].fileNo, indexList[index].offset, indexList[index].datasize)
	// 		hash1 := sha256.Sum256(dataList[index])
	// 		hash2 := sha256.Sum256(copydata)
	// 		if hash1 != hash2 {
	// 			t.Errorf("wrong in %d", index)
	// 		}
	// 	}(i)
	// }
}

func TestFilepoolGorutine(t *testing.T) {
	gorutineLimit := 20
	semaphore := make(chan struct{}, gorutineLimit)
	// backup.initFilepool()
	num := 5000000
	filepool := backup.NewFilepool(10, 64*1024)
	dataList := make([][]byte, num)
	indexList := make([]*backup.DataIndex, num)
	var wg sync.WaitGroup
	for i := 0; i < num; i++ {
		dataList[i] = make([]byte, 4*1024)
		rand.Read(dataList[i])
		wg.Add(1)
		go func(index int) {
			semaphore <- struct{}{}
			defer wg.Done()
			defer func() {
				<-semaphore
			}()
			fileNo, offset, datasize := filepool.WriteToDisk(dataList[index])
			indexList[index] = &backup.DataIndex{FileNo: fileNo, Offset: offset, Datasize: datasize}

		}(i)
	}
	wg.Wait()
	for i := 0; i < num; i++ {
		go func(index int) {
			semaphore <- struct{}{}
			defer func() {
				<-semaphore
			}()
			copydata := filepool.ReadFromDisk(indexList[index].FileNo, indexList[index].Offset, indexList[index].Datasize)
			hash1 := sha256.Sum256(dataList[index])
			hash2 := sha256.Sum256(copydata)
			if hash1 != hash2 {
				t.Errorf("wrong in %d", index)
			}
		}(i)
	}
}
