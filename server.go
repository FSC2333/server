package main

import (
	lruCache "ECDedup/server/cache"
	"encoding/binary"
	"fmt"
	"net"
	"sync" //用于协调不同的goroutine之间同步操作

	"github.com/vmihailenco/msgpack/v5"
)

var gorutineLimit = 50

func startServer(port string) {
	listener, err := net.Listen("tcp", ":"+port) //使用TCP协议，监听所有可用IP地址的8080端口。
	//底层实现包括了创建套接字和绑定到指定的IP地址和端口

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		return
	}
	defer listener.Close()
	fmt.Println("Server listening on :", port)
	cache := lruCache.New(64*1024, 10, 4*1024*1024)

	for {
		conn, err := listener.Accept()
		/*
			从监听套接字的已完成连接队列中取出第一个连接

			创建一个新的套接字描述符指向这个连接
		*/
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			return
		}
		go handleConnection(conn, cache) //用一个goroutine来处理，listener继续监听这个端口（公用一个cache）
	}
}

func handleConnection(conn net.Conn, cache *lruCache.LruCache) {
	//从连接中读取到的数据 1方法（GET/PUT） 2 HASH数据的长度  3HASH数据的二进制信息
	defer conn.Close()
	method := make([]byte, 3)
	dataLengthBuffer := make([]byte, 8)             //8个字节用于表示数据长度
	semaphore := make(chan struct{}, gorutineLimit) // 管道的类型是空结构体，好处是不占用内存，并且可以表示一个信号量
	var wg sync.WaitGroup
	_, err := conn.Read(method) //从连接中读取数据到method缓冲区中，返回成功读取到的字节数和错误信息
	if err != nil {
		fmt.Println("Error reading PUT instruction:", err.Error())
		return
	}
	fmt.Println(string(method), " method")
	if string(method) == "PUT" {
		_, err = conn.Read(dataLengthBuffer) //每一次都从上次读取完的结尾的下一个字节进行读取
		if err != nil {
			fmt.Println("Error reading data length:", err.Error())
			return
		}
		dataLength := binary.BigEndian.Uint64(dataLengthBuffer) //将8字节的网络字节序（大端序）数据转换为本机的uint64整型

		binaryHashList := make([]byte, dataLength)
		// _, err = conn.Read(binaryHashList)
		_, err = readAll(conn, binaryHashList)
		if err != nil {
			fmt.Println("Error reading data:", err.Error())
			return
		}
		var hashList [][]byte
		err := msgpack.Unmarshal(binaryHashList, &hashList) //解码后存储在HashList中，（有多个Hash值）
		if err != nil {
			panic(err)
		}
		hashBoolList := make([]bool, len(hashList)) //每一个hash值对应一个bool值
		for i := 0; i < len(hashList); i++ {
			wg.Add(1)
			go func(i int) {
				semaphore <- struct{}{} //第一个括号表示类型是空结构体，第二个括号表示结构体中的值（就是空）
				defer func() {
					<-semaphore
				}()
				//为什么会使用匿名函数来实现信号量的释放：？因为如果Query操作发生了panic，会导致<-semaphore操作永远无法进行，defer保证了函数
				/*函数正常返回时
				函数发生 panic 时
				函数运行时错误导致退出时执行*/
				defer wg.Done()
				hashBoolList[i] = cache.Query(string(hashList[i])) //defer：后进先出
			}(i)
		}
		wg.Wait()
		var NoList []int //记录向cache查询结果为false的哈希值的下标，切片

		for i := 0; i < len(hashList); i++ {
			if !hashBoolList[i] {
				NoList = append(NoList, i) //切片可以使用append动态扩展
			}
		}

		binaryNoList, err := msgpack.Marshal(NoList) //NoList格式转化为二进制格式
		if err != nil {
			panic(err)
		}

		reply := make([]byte, 8)
		binary.BigEndian.PutUint64(reply, uint64(len(binaryNoList)))
		/*
			将binaryNoList的长度(元素个数)转换为64位无符号整数
			使用大端序(BigEndian)将这个长度值写入reply的前8个字节
			这是网络通信中常见的做法，因为网络协议通常要求使用大端序
		*/
		reply = append(reply, binaryNoList...)
		/*现在reply的结构是：前8字节是长度信息，后面是实际数据内容*/
		conn.Write(reply)

		/*| PUT | HashListLen | HashList | UniqueDataLen 8| UniqueHashLen8 | UniqueData | UniqueHash |*/
		_, err = conn.Read(dataLengthBuffer)
		if err != nil {
			fmt.Println("Error reading data length:", err.Error())
			return
		}
		binaryUniqueDataListLength := binary.BigEndian.Uint64(dataLengthBuffer)
		_, err = conn.Read(dataLengthBuffer)
		if err != nil {
			fmt.Println("Error reading data length:", err.Error())
			return
		}
		// 补发没有在缓存中查询到的数据及其哈希值
		binaryUniqueHashListLength := binary.BigEndian.Uint64(dataLengthBuffer) //dataLengthBuffer 重复用了两次，第一次是

		binaryUniqueDataList := make([]byte, binaryUniqueDataListLength)
		binaryUniqueHashList := make([]byte, binaryUniqueHashListLength)
		// _, err = conn.Read(binaryUniqueDataList)
		_, err = readAll(conn, binaryUniqueDataList) //数据
		if err != nil {
			fmt.Println("Error reading data length:", err.Error())
			return
		}
		// n, err := conn.Read(binaryUniqueHashList)
		_, err = readAll(conn, binaryUniqueHashList) //数据的哈希值
		if err != nil {
			fmt.Println("Error reading data length:", err.Error())
			return
		}
		var uniqueDataList, uniqueHashList [][]byte
		err = msgpack.Unmarshal(binaryUniqueDataList, &uniqueDataList)
		// fmt.Println(binaryUniqueDataListLength)
		if err != nil {
			panic(err)
		}
		err = msgpack.Unmarshal(binaryUniqueHashList, &uniqueHashList) //
		/*
			在 Go 中，msgpack.Unmarshal 能够将一维字节切片（[]byte）反序列化为二维字节切片（[][]byte），
			是因为 msgpack（或类似的序列化协议，如 JSON、protobuf）在编码时已经存储了数据的 结构化信息，而不仅仅是原始字节流。
		*/
		if err != nil {
			panic(err)
		}
		for i := 0; i < len(uniqueHashList); i++ {
			// wg.Add(1)
			go func(i int) {
				semaphore <- struct{}{}
				defer func() {
					<-semaphore
				}()
				// defer wg.Done()
				cache.Add(string(uniqueHashList[i]), *lruCache.NewByteView(uniqueDataList[i]))
			}(i)

		}

	} else if string(method) == "GET" {
		_, err := conn.Read(dataLengthBuffer) //dataLengthBuffer := make([]byte, 8) 上文定义
		if err != nil {
			panic(err)
		}
		binaryHashListLength := binary.BigEndian.Uint64(dataLengthBuffer)
		//binary.BigEndian.Uint64 将字节切片 dataLengthBuffer 解释为一个无符号的 64 位整数
		binaryHashList := make([]byte, binaryHashListLength)
		_, err = readAll(conn, binaryHashList) //从连接中不断读取数据，直到把binaryHashList数组填满
		if err != nil {
			panic(err)
		}
		var HashList [][]byte
		err = msgpack.Unmarshal(binaryHashList, &HashList) //解码后存储在HashList中，（有多个Hash值）
		if err != nil {
			panic(err)
		}
		dataList := make([][]byte, len(HashList))
		for i := 0; i < len(HashList); i++ {
			wg.Add(1)
			go func(i int) {
				semaphore <- struct{}{}
				defer func() {
					<-semaphore
				}()
				defer wg.Done()
				temp, _ := cache.Get(string(HashList[i])) //从LRU缓存中取
				dataList[i] = temp.ByteSlice()
			}(i)

		}
		wg.Wait()

		binaryDataList, err := msgpack.Marshal(dataList) //转化为二进制数据
		replay := make([]byte, 8)
		binary.BigEndian.PutUint64(replay, uint64(len(binaryDataList))) //开始的八个字节表示长度
		replay = append(replay, binaryDataList...)
		conn.Write(replay)

	}
}

func main() {
	// ports := os.Args[1:]
	ports := []string{"8000"}
	for _, port := range ports {
		go startServer(port)
	}
	select {}
}

func readAll(conn net.Conn, buffer []byte) (int, error) {
	total := 0
	for total < len(buffer) {
		n, err := conn.Read(buffer[total:])
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
