package backup

import (
	"os"

	"github.com/syndtr/goleveldb/leveldb"
)

var dbpath = "./dbpath/"

type Storage struct {
	filepool Filepool
	db       *leveldb.DB
}

func New(poolsize int64, maxfilesize int64) *Storage {
	initSorage()
	db, err := leveldb.OpenFile(dbpath, nil) //创建数据库实例
	if err != nil {
		panic(err)
	}
	return &Storage{
		filepool: *NewFilepool(poolsize, maxfilesize), //文件池的容量和限制单个文件池的最大大小
		db:       db,
	}
}

func (s *Storage) Put(key, value []byte) {
	fileNo, offset, datasize := s.filepool.WriteToDisk(value)
	err := s.db.Put(key, marshalDataIndex(&DataIndex{FileNo: fileNo, Offset: offset, Datasize: datasize}), nil)
	if err != nil {
		panic(err)
	}
}
func (s *Storage) Get(key []byte) []byte {
	marshalDataIndex, err := s.db.Get(key, nil)
	if err != nil {
		panic(err)
	}
	dataIndex := unmarshalDataIndex(marshalDataIndex)
	return s.filepool.ReadFromDisk(dataIndex.FileNo, dataIndex.Offset, dataIndex.Datasize)
}

func (s *Storage) Query(key []byte) bool {
	_, err := s.db.Get(key, nil)
	return err == nil
}

func initSorage() {
	os.RemoveAll(dbpath)
	os.Mkdir(dbpath, 0755)
}

/*
os.RemoveAll(dbpath)
强制删除 dbpath 路径指向的目录（如果存在）
会递归删除该目录下的所有文件和子目录
如果目录不存在也不会报错

os.Mkdir(dbpath, 0755)
重新创建一个全新的空目录
0755 是目录权限（属主可读写执行，其他人可读执行）
*/
