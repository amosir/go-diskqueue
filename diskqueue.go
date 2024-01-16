package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	PeekChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// 当前读取位置
	readPos int64
	// 当前写入位置
	writePos int64
	// 读取位置所在的文件编号
	readFileNum int64
	// 写入位置所在的文件编号
	writeFileNum int64
	// 队列中的消息数
	depth int64

	sync.RWMutex

	// instantiation time metadata
	name string
	// 数据保存的目录
	dataPath string
	// 单文件可保存的最大字节数，超过这个值要创建新文件
	maxBytesPerFile int64 // cannot change once created
	// 可从当前读取文件中读的最大数据大小
	maxBytesPerFileRead int64
	// 消息大小的最小值
	minMsgSize int32
	// 消息大小的最大值
	maxMsgSize int32
	// 指定写入多少条消息后需要进行一次 fsync 操作
	syncEvery int64 // number of writes per fsync
	// 定时刷盘的周期
	syncTimeout time.Duration // duration of time per fsync
	exitFlag    int32
	// 是否需要刷盘
	needSync bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	// 下一次读取位置
	nextReadPos int64
	// 下一次写入位置
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	// 待读取文件对应的 reader 对象(带缓冲的)
	reader *bufio.Reader
	// 写缓存，向文件中写入数据前，会先将数据写入到缓存中
	writeBuf bytes.Buffer

	// ReadChan() 返回此 channel，用于读取队列中的数据
	readChan chan []byte

	// exposed via PeekChan()
	peekChan chan []byte

	// internal channels
	depthChan chan int64
	// 调用 Put 方法写入数据时，会先将数据保存在 writeChan，再由 ioloop 协程异步处理
	writeChan chan []byte
	// Put 方法阻塞在此 channel，直到 ioLoop 处理完
	writeResponseChan chan error
	// 调用 Empty 方法清空数据时，通过 emptyChan 向 ioloop 协程发送指令
	emptyChan         chan int
	emptyResponseChan chan error
	// 关闭 channel 时，通过此 channel 向 ioLoop 发送退出指令
	exitChan     chan int
	exitSyncChan chan int

	logf AppLogFunc
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		peekChan:          make(chan []byte),
		depthChan:         make(chan int64),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
	}

	// 从元数据文件中恢复上一次读写的位置信息，包括: 读写文件编号、读写位置和队列中数据量
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()
	return &d
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthChan
	if !ok {
		// ioLoop exited
		depth = d.depth
	}
	return depth
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

func (d *diskQueue) PeekChan() <-chan []byte {
	return d.peekChan
}

func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	// 将数据写入到 writeChan，由 ioLoop 协程处理
	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	// 这里关闭 exitChan后，ioLoop 协程会退出执行，并向 exitSyncChan 写入数据以同步退出进度
	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	close(d.depthChan)

	// 关闭读写文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	// ioLoop 接收到 channel 数据后，执行退出操作
	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	// 删除数据文件
	err := d.skipToNextRWFile()

	// 删除元数据文件
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	// 关闭当前读写文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	// 删除所有数据文件
	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	// 指定新的读写文件位置
	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	// 打开待读取文件并定位到目标位置
	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		// for "complete" files (i.e. not the "current" file), maxBytesPerFileRead
		// should be initialized to the file's size, or default to maxBytesPerFile
		// 当前文件可读取的最大字节数不超过文件大小
		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err == nil {
				d.maxBytesPerFileRead = stat.Size()
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	// 数据长度占 4 字节
	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	// 根据上面确定的数据长度读取数据
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// 移动下一次读取的位置，当前读取位置不变
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// we only consider rotating if we're reading a "complete" file
	// and since we cannot know the size at which it was rotated, we
	// rely on maxBytesPerFileRead rather than maxBytesPerFile
	// 如果一个文件读完了，则定位到下一个文件的开头
	if d.readFileNum < d.writeFileNum && d.nextReadPos >= d.maxBytesPerFileRead {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}
		// 定位到下一个文件
		d.nextReadFileNum++
		d.nextReadPos = 0
	}
	// 返回读取到的数据
	return readBuf, nil
}

// 用于将数据写入底层的磁盘文件中
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	// 数据大小检查
	dataLen := int32(len(data))
	// 完整的数据包括: 4 字节的长度 + 实际数据
	totalBytes := int64(4 + dataLen)

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", dataLen, d.minMsgSize, d.maxMsgSize)
	}

	// 如果这条消息写入文件后，导致文件大小超过 maxBytesPerFile，则该消息不能写入到当前文件，而应该创建并写入到新文件
	if d.writePos > 0 && d.writePos+totalBytes > d.maxBytesPerFile {
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		// 将写入位置设置到下一个文件的开头
		d.writeFileNum++
		d.writePos = 0

		// 每次写入到新文件时，都要将元数据信息和原始写入文件落盘
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}
	// 创建或打开当前写入文件，并定位到写入位置
	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	d.writeBuf.Reset()
	// 向写缓存中写入待写入数据的长度
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	// 向写缓存中写入待写入数据
	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// 将写缓存的数据写到文件
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	// 更新写入位置
	d.writePos += totalBytes
	// 更新消息数
	d.depth += 1

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	// 读取元数据文件
	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 基于元数据文件内保存的信息，初始化磁盘队列
	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	d.depth = depth
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	// if the metadata was not sync'd at the last shutdown of nsqd
	// then the actual file size might actually be larger than the writePos,
	// in which case the safest thing to do is skip to the next file for
	// writes, and let the reader salvage what it can from the messages in the
	// diskqueue beyond the metadata's likely also stale readPos
	// 如果数据写入文件之后，元数据信息刷新到磁盘之前宕机，则元数据中记录的位置信息就可能是错的
	// 这里将写入位置移动到下一个文件
	fileName = d.fileName(d.writeFileNum)
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if d.writePos < fileSize {
		d.logf(WARN,
			"DISKQUEUE(%s) %s metadata writePos %d < file size of %d, skipping to new file",
			d.name, fileName, d.writePos, fileSize)
		d.writeFileNum += 1
		d.writePos = 0
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		d.depth,
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	// 如果还有消息未读，直接返回
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		d.depth = 0
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	// 更新读取位置
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	// 消息数减一
	d.depth -= 1

	// 当前读取文件和下一次读取文件不同，说明当前文件已经读完了，可以删除
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}
	}
	// 检查尾部数据的完整性，如果有问题则跳过这部分数据
	d.checkTailCorruption(d.depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true

	d.checkTailCorruption(d.depth)
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	// 记录上次刷盘后新写入/读取的消息数
	var count int64
	var r chan []byte
	var p chan []byte

	// 启动定时器定时刷盘
	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// 每写入 syncEvery 条数据就进行一次 sync 操作
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			// 对文件进行 sync 操作刷盘
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			// sync 后 count 清零
			count = 0
		}

		// 文件中有未读的数据
		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			// 什么情况下 nextReadPos == readPos，什么情况下不等于
			// readOne 之后 nextReadPos 指向下一个位置，此时 nextReadPos != readPos
			// 有协程从 readChan 读取数据之后，moveForward 将 readPos 移动到 nextReadPos，此时相同
			// 不相等说明数据从文件读到内存了，但是内存中的数据还没读取
			if d.nextReadPos == d.readPos {
				// 从文件中读取一条消息
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
			p = d.peekChan
		} else {
			// 这里表明没有未读的数据
			r = nil
			p = nil
		}

		select {
		// 上面读取到了一条消息，放到 readChan 中，readChan 会暴露给外部
		case p <- dataRead:
		case r <- dataRead:
			count++
			// 更新readPos, readFileNum, nextReadPos, nextReadFileNum的信息
			d.moveForward()
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			// 执行清空操作(Empty函数)时，会向 emptyChan 写入数据，进入此分支
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			// 当有数据写入(如调用 Put 方法)的时候，并没有直接将数据直接写入文件，而是先写入writeChan，然后在这里调用 writeOne 写入文件
			count++
			// 写入完毕后，将响应写回 writeResponseChan，从而解除写入协程的阻塞
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			// 达到指定刷盘间隔，此时需要将元数据和文件中的数据刷盘。
			// count 为 0  表示上次刷盘后没有新数据写入，则不需执行刷盘操作
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
