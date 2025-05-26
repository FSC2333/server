package lruCache

import (
	"ECDedup/server/backup"
	"container/list"
	"fmt"
	"sync"
)

type entry struct {
	key   string
	value ByteView
}

type LruCache struct {
	mu      sync.Mutex               //互斥锁
	lruList *list.List               //双向链表
	cache   map[string]*list.Element //哈希表，
	maxsize int64                    //最大容量
	cursize int64                    //当前大小
	storage backup.Storage           //持久化存储接口
}

func New(maxzie, poolsize, maxfilesize int64) *LruCache {
	return &LruCache{
		mu:      sync.Mutex{}, //零值互斥锁，可以直接当成一个未锁定状态的互斥锁来使用
		lruList: list.New(),
		cache:   make(map[string]*list.Element),
		maxsize: int64(maxzie),
		cursize: 0,
		storage: *backup.New(poolsize, maxfilesize),
	}
}

func (c *LruCache) Add(key string, value ByteView) {
	c.mu.Lock()

	//Vcontainer/list 包中，list.Element 的 Value 字段是一个 interface{} 类型，它可以存储任意类型的值，需要对其进行断言操作
	defer c.mu.Unlock()
	if ele, ok := c.cache[key]; ok {
		c.lruList.MoveToFront(ele)
		entry_tmp := ele.Value.(*entry) //类型断言，相当于告诉编译器："我知道这个 interface{} 实际是 *entry 类型，请把它当作这种类型来使用"。返回entry指针
		c.cursize += int64(value.Len()) - int64(entry_tmp.value.Len())
		ele.Value = &entry{key, value}
	} else {
		ele := c.lruList.PushFront(&entry{key, value}) //返回新插入的元素的指针
		c.cache[key] = ele
		c.cursize += int64(value.Len())
	}

	c.storage.Put([]byte(key), value.ByteSlice()) //直写，保证数据的一致性

	if c.cursize > c.maxsize {
		ele := c.lruList.Back()
		if ele != nil {
			entry := ele.Value.(*entry)
			c.lruList.Remove(ele)
			delete(c.cache, entry.key) //从map中删除键值对
			c.cursize -= int64(entry.value.Len())
		}
	}
}

func (c *LruCache) Get(key string) (value ByteView, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ele, ok := c.cache[key]; ok {
		fmt.Println("hit")
		c.lruList.MoveToFront(ele)
		entry := ele.Value.(*entry)
		return entry.value, true
	} else {
		value = *NewByteView(c.storage.Get([]byte(key)))
		if value.b != nil {
			ele := c.lruList.PushFront(&entry{key, value})
			c.cache[key] = ele
			c.cursize += int64(value.Len())
			if c.cursize > c.maxsize {
				ele := c.lruList.Back()
				if ele != nil {
					c.lruList.Remove(ele)
					entry := ele.Value.(*entry)
					delete(c.cache, entry.key)
					c.cursize -= int64(entry.value.Len())
				}
			}
			return value, true
		}
	}
	return
}

func (c *LruCache) Query(key string) bool {
	return c.storage.Query([]byte(key))
}
