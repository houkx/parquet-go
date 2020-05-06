package parquet

import (
	"compress/flate"
	"compress/gzip"
	"github.com/valyala/bytebufferpool"
	"io/ioutil"
	"sync"
)

var bufferPool = sync.Pool{
	New: func() interface{} {
		return new(Buffer)
	},
}

func GetBuffer() *bytebufferpool.ByteBuffer {
	return bytebufferpool.Get()
}

//
func PutBuffer(buf *bytebufferpool.ByteBuffer) {
	bytebufferpool.Put(buf)
}
func getBuffer() *Buffer {
	return bufferPool.Get().(*Buffer)
}

//
func putBuffer(buf *Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

var writerGzipPool = sync.Pool{
	New: func() interface{} {
		//return gzip.NewWriter(ioutil.Discard)
		w, _ := gzip.NewWriterLevel(ioutil.Discard, flate.BestSpeed)
		return w
	},
}

func GetGzipWriter() *gzip.Writer {
	return writerGzipPool.Get().(*gzip.Writer)
}

func PutGzipWriter(buf *gzip.Writer) {
	buf.Flush()
	writerGzipPool.Put(buf)
}
