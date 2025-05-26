package backup

import (
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var globalRand = rand.New(rand.NewSource(time.Now().UnixNano()))

type Filepool struct {
	poolsize    int64
	curnum      int64
	maxfilesize int64
	filelist    []*Fileview
	muList      []sync.Mutex
}

func NewFilepool(poolsize int64, maxfilesize int64) *Filepool {
	filepool := &Filepool{
		poolsize:    poolsize,
		curnum:      poolsize - 1,
		maxfilesize: maxfilesize,
		// rwmu:        sync.RWMutex{},
		filelist: make([]*Fileview, poolsize),
		muList:   make([]sync.Mutex, poolsize),
	}
	initFilepool()
	var i int64
	for i = 0; i < poolsize; i++ {
		filepool.filelist[i] = NewFileView(i)
		filepool.muList[i] = sync.Mutex{}
	}
	return filepool
}

func (filepool *Filepool) WriteToDisk(value []byte) (fileNo, offset, datasize int64) {
	index := globalRand.Int63n(filepool.poolsize) //随机选择一个文件索引（0 到 poolsize-1）
	for !filepool.muList[index].TryLock() {
		index = (index + 1) % filepool.poolsize //这个索引所对应的文件被加锁，换一个索引
	}
	datasize = int64(len(value))
	defer filepool.muList[index].Unlock()
	fileNo = filepool.filelist[index].GetFileID()
	file, err := os.OpenFile(Filedir+strconv.FormatInt(fileNo, 10), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	/*
		os.O_APPEND：写入时追加到文件末尾。
		os.O_WRONLY：只写模式。
		os.O_CREATE：如果文件不存在则创建。
	*/
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	offset, err = file.Seek(0, io.SeekEnd) //获取当前文件偏移量，将指针移动到文件末尾
	if err != nil {
		panic(err.Error())
	}

	file.Write(value)

	if filepool.filelist[index].Write(datasize) >= filepool.maxfilesize {
		filepool.filelist[index] = NewFileView(fileNo + filepool.poolsize)
	}
	return fileNo, offset, datasize
}

func (filepool *Filepool) ReadFromDisk(fileNo, offset, datasize int64) (value []byte) {
	file, err := os.Open(Filedir + strconv.FormatInt(fileNo, 10))
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err.Error())
	}
	data := make([]byte, datasize)

	_, err = file.Read(data)
	if err != nil {
		panic(err.Error())
	}
	return data
}

func initFilepool() {
	os.RemoveAll(Filedir)
	os.Mkdir(Filedir, 0755)
}
