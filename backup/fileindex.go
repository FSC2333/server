package backup

import (
	"github.com/vmihailenco/msgpack/v5"
)

type DataIndex struct {
	FileNo   int64
	Offset   int64
	Datasize int64
}

func marshalDataIndex(fileindex *DataIndex) []byte {
	b, err := msgpack.Marshal(fileindex)
	if err != nil {
		panic(err)
	}
	return b
}

func unmarshalDataIndex(b []byte) *DataIndex {
	var fileIndex DataIndex
	err := msgpack.Unmarshal(b, &fileIndex)
	if err != nil {
		panic(err)
	}
	return &fileIndex
}
