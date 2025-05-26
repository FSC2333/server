package backup

import (
	"os"
	"strconv"
)

const Filedir = "./shards/"

type Fileview struct {
	fileID   int64
	filesize int64
}

func NewFileView(fileID int64) *Fileview {
	fileview := &Fileview{
		fileID: fileID,
		// mu:       sync.Mutex{},
		filesize: 0,
	}
	file, err := os.Create(Filedir + strconv.FormatInt(fileID, 10)) //将fileID转换为字符串形式，然后与Filedir拼接形成完整的文件路径
	defer file.Close()
	if err != nil {
		panic(err.Error())
	}
	return fileview
}

func (fileview *Fileview) Write(shardsize int64) int64 {
	fileview.filesize += shardsize
	// fileview.mu.Unlock()
	return fileview.filesize
}

func (fileview *Fileview) GetFileID() int64 {
	return fileview.fileID
}
